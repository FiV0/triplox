# Triplox OVHcloud stress-test deployment

A small, stress-testable OVHcloud Public Cloud deployment that mirrors
`docker/docker-compose-kafka.yml` on real infrastructure. It is the OVH sibling of
the AWS rig in [`deploy/terraform/`](../terraform); same three roles, each on its
own instance (so a stress test attributes bottlenecks cleanly):

- **AutoMQ** — single-node Kafka-on-S3 broker (`r3-16`).
- **Triplox** — the database server under test (`b3-16`, dedicated NVMe cache volume).
- **Load-gen** — optional AuctionMark benchmark (`b3-8`), **off by default**.

Storage is three OVH S3-compatible buckets (Triplox SlateDB + AutoMQ data/ops).
The nodes share a vRack-backed private network for inter-node traffic. Admin access
is via SSH. State is local (`terraform.tfstate`, gitignored).

Works with OpenTofu (`tofu`) or Terraform (`terraform`) — plain HCL.

## How this differs from the AWS rig

OVH Public Cloud is OpenStack underneath, so this stack uses **two providers**:

- **`ovh/ovh`** for the account-plane — the three S3 buckets, the S3 user + keys +
  policy, and the private network/subnet.
- **`terraform-provider-openstack/openstack`** for the compute-plane — the three
  instances, the SSH keypair, security groups, and the cache block volume.
  (`ovh_cloud_project_instance` exists but is Beta: no security groups, public-only
  networking — `openstack_compute_instance_v2` is the capable path.)

Other deliberate departures from AWS, all noted inline in the code:

- **No SSM.** Access is plain **SSH** with a keypair you provide (`ssh_public_key`).
- **The host firewall (`ufw`), not security groups, is the real inbound gate.** OVH
  does not reliably enforce security groups on the public network, and Docker's port
  mapping bypasses them regardless. So AutoMQ and Triplox run with `--network host`
  and each node's `ufw` locks SSH and the Triplox port to `admin_cidr`. Security
  groups are still defined (they are enforced on the private network).
- **No secret manager.** AWS stashed the S3 keys in SSM SecureString; OVH has no
  equivalent here, so the keys are rendered into the nodes' `user_data` (and the
  local `tfstate`). Fine for a throwaway bench rig; tighten if you reuse this.
- **Region casing trips people up:** OpenStack uses `GRA11`, the storage resource
  uses `GRA`, and the S3 SDK/endpoint use `gra`. `storage_region` is the uppercase
  one; the lowercase form is derived.

## Prerequisites

1. **An OVH Public Cloud project** and its project id (`project_id`).
2. **OVH API credentials** for the `ovh` provider, exported in your environment:
   ```bash
   export OVH_ENDPOINT=ovh-eu
   export OVH_APPLICATION_KEY=...
   export OVH_APPLICATION_SECRET=...
   export OVH_CONSUMER_KEY=...
   ```
   Create them at https://api.ovh.com/createToken/ (or use OAuth2 `OVH_CLIENT_ID` /
   `OVH_CLIENT_SECRET`).
3. **OpenStack credentials** for the `openstack` provider. Download an OpenStack user's
   `openrc.sh` (or `clouds.yaml`) from the OVH console and source it, or export an
   application credential:
   ```bash
   source openrc.sh                       # sets OS_* env vars
   # or:
   export OS_AUTH_URL=https://auth.cloud.ovh.net/v3
   export OS_APPLICATION_CREDENTIAL_ID=...
   export OS_APPLICATION_CREDENTIAL_SECRET=...
   ```
4. **A kafka-enabled Triplox image on GHCR.** The published image does *not* include
   the `kafka` feature; the `Build and push (kafka)` step in
   `.github/workflows/docker-publish.yml` publishes `ghcr.io/fiv0/triplox:<ver>-kafka`
   (and `:snapshot-kafka`). Run that workflow and confirm the tag is public:
   ```bash
   docker pull ghcr.io/fiv0/triplox:snapshot-kafka
   ```
5. **OpenTofu** ≥ 1.6 (`tofu`).

## Run

```bash
cd deploy/terraform-ovh
cp terraform.tfvars.example terraform.tfvars   # set project_id, ssh_public_key, admin_cidr

tofu init
tofu plan
tofu apply
```

`admin_cidr` **must** be tightened to your own IP — port 5490 is an unauthenticated
protocol. Apply takes a few minutes; the instances finish provisioning via user_data
(Docker pulls + AutoMQ topic creation) shortly after they boot.

## Verify

```bash
tofu output                       # endpoint, bucket names, ssh commands

# Shell onto a node:
ssh debian@$(tofu output -raw triplox_public_ip)
sudo cat /var/log/triplox-userdata.log   # provisioning log
sudo docker logs automq                   # broker up; topic created   (on automq)
sudo docker logs triplox                  # kafka mode resolved, listening (on triplox)
```

Then connect a Triplox client to the `triplox_endpoint` output and run a transaction;
objects should appear in the `triplox` bucket and records in `automq-data`.

## Stress with the load generator

Once the AuctionMark image exists:

```bash
tofu apply -var enable_load_gen=true -var loadgen_image=<image>
# SSH onto the loadgen host, then:
LOADGEN_IMAGE=<image> /usr/local/bin/run-loadgen.sh
```

Scale via `loadgen_scale_factor`, `loadgen_threads`, `loadgen_duration`. Scale the
Triplox node (the unit under test) via `-var triplox_flavor=b3-32`, or move the cache
onto instance-local NVMe with `-var triplox_flavor=i1-45 -var triplox_disk_mode=local`.

## Teardown

```bash
tofu destroy
```

Empty the buckets first if `destroy` refuses (OVH will not delete a non-empty bucket):
the rig writes objects into all three.

## Gotchas worth knowing

- **Private network attaches by name.** The instances reference the OVH private
  network by its `name`. If the OpenStack-visible name ends up differing in your
  region, switch the instance `network {}` blocks to the network's `openstackid`
  (exported under `regions_attributes`).
- **`fixed_ip_v4` is set outside the DHCP pool** (`.10`–`.12`, pool starts at `.100`).
  The addresses must stay inside `private_subnet_cidr`.
- **AutoMQ talks to OVH S3 virtual-hosted-style** (the documented default). If the
  broker can't reach buckets, add `&pathStyle=true` to the `s3.*.buckets` URLs in
  `templates/automq_user_data.sh.tftpl`.
