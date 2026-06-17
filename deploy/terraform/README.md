# Triplox AWS stress-test deployment

A small, stress-testable AWS deployment that mirrors `docker/docker-compose-kafka.yml`
on real infrastructure. Three roles, each on its own EC2 instance (so a stress test
attributes bottlenecks cleanly):

- **AutoMQ** — single-node Kafka-on-S3 broker (`r6i.large`).
- **Triplox** — the database server under test (`m6id.xlarge`, local NVMe cache).
- **Load-gen** — optional AuctionMark benchmark (`c6i.xlarge`), **off by default**.

Storage is three S3 buckets (Triplox SlateDB + AutoMQ data/ops). Admin access is via
SSM Session Manager (no SSH). State is local (`terraform.tfstate`, gitignored).

Works with OpenTofu (`tofu`) or Terraform (`terraform`) — plain HCL, no Cloud-only features.

## Prerequisites

1. **AWS credentials** with permission to create VPC/EC2/S3/IAM/SSM resources, exported
   in your environment (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`) or
   an `~/.aws` profile.
2. **A published Triplox image on GHCR.** The published image includes the `kafka`
   feature, so the default `ghcr.io/fiv0/triplox:snapshot` (or a version tag) works
   as-is. Confirm it is pullable before applying:
   ```bash
   docker pull ghcr.io/fiv0/triplox:snapshot
   ```
3. **OpenTofu** ≥ 1.6 (`tofu`).

## Run

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # set triplox_image (admin_cidr is optional)

tofu init
tofu plan
tofu apply
```

By default **no public 5490 ingress is opened** — reach Triplox via the SSM
port-forward tunnel (below) or from inside the VPC. Set `admin_cidr` to your own
IP/32 only if you want direct public access; the protocol is unauthenticated, so
never leave it at a wide range. Apply takes a few minutes; the instances finish
provisioning via user_data (Docker pulls + AutoMQ topic creation) after boot.

## Shell access (SSM Session Manager)

There is no SSH. Shell access is via SSM Session Manager, which needs the
**Session Manager plugin** installed locally (separate from the AWS CLI):

```bash
# Arch / Manjaro:
yay -S aws-session-manager-plugin        # or: paru -S aws-session-manager-plugin
# Other distros: https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
session-manager-plugin --version         # verify
```

`tofu output ssm_sessions` prints the ready-to-paste command for each node:

For example
```bash
aws ssm start-session --region eu-central-1 --target i-<triplox>
```

Inside the session you can then observe the logs of an instance.
```sh
sudo docker ps
sudo docker logs --tail 50 triplox
```

### Reach Triplox on localhost (no public ingress)

Port-forward 5490 over SSM — this is the default access path and needs no
inbound rule (works with `admin_cidr` unset):

```bash
aws ssm start-session --region eu-central-1 --target i-<triplox> \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["5490"],"localPortNumber":["5490"]}'
```

Leave it running and point a client at `localhost:5490`.

## Verify

```bash
tofu output                       # endpoint, bucket names, ssm_sessions

# On the AutoMQ node:
sudo docker logs automq           # broker up; tx-log topic created
# On the Triplox node:
sudo docker logs triplox          # kafka mode resolved, listening on 5490 (no crash-loop)
sudo cat /var/log/triplox-userdata.log   # provisioning log if a container is missing
```

## Stress with the load generator

Once a load-gen image exists (for example Auctionmark):

```bash
tofu apply -var enable_load_gen=true -var loadgen_image=<image>
# SSM onto the loadgen host, then:
LOADGEN_IMAGE=<image> /usr/local/bin/run-loadgen.sh
```

Scale via `loadgen_scale_factor`, `loadgen_threads`, `loadgen_duration`. Scale the
Triplox node (the unit under test) via `-var triplox_instance_type=m6id.2xlarge`.

## Teardown

```bash
tofu destroy
```

Buckets use `force_destroy = true` so destroy works without emptying them first.
