---
title: Operations
description: Operating Triplox in production.
---

### Deployment

:::note
This section is currently very much WIP. Triplox currently only comes as a writer node.
The binary currently only supports connecting to S3-compatible object storage.
GCP and Azure configurations will follow.
:::

Triplox comes as a docker image. The images are published to the
[GitHub Container Registry](https://github.com/FiV0/triplox/pkgs/container/triplox). Please
check there to make sure you are using the latest version when initially testing. See also the [Quick Start](/getting-started/quick-start/) section.

```bash
docker run --rm -p 5490:5490 ghcr.io/fiv0/triplox:0.1.0-alpha.8
```

The image includes configurations for the following storage modes:

| Mode | Storage | Transaction log | Persistence |
|---|---|---|---|
| `dev` | In memory | In memory | A fresh database is created for every connection. |
| `memory` | In memory | In memory | Data is lost when the container stops. |
| `local` | Local filesystem | Local file | Data is stored under `/var/lib/triplox`. |
| `remote` | object storage | Local file | Database storage is remote; the transaction log and caches are local. |
| `kafka` |  object storage | Kafka | Database storage and the transaction log are external. |

`memory` is the default. Select another bundled configuration with the
`TRIPLOX_STORAGE` environment variable. A persistent local node requires a
volume:

```bash
docker run --rm -p 5490:5490 \
  -e TRIPLOX_STORAGE=local \
  -v triplox-data:/var/lib/triplox \
  ghcr.io/fiv0/triplox:0.1.0-alpha.8
```

#### Custom configuration

`TRIPLOX_STORAGE` selects a bundled configuration; it does not override
individual configuration fields. To configure paths, ports, object-storage
credentials, or Kafka endpoints, mount a TOML file and pass its container path
as the image argument:

```bash
docker run --rm \
  -p 5490:5490 \
  --mount type=bind,src="$(pwd)/triplox.toml",dst=/etc/triplox/custom.toml,readonly \
  --mount type=volume,src=triplox-data,dst=/var/lib/triplox \
  ghcr.io/fiv0/triplox:0.1.0-alpha.8 \
  /etc/triplox/custom.toml
```

For example, a persistent local configuration is:

```toml
[storage]
type = "local"
path = "/var/lib/triplox/data"

[log]
type = "file"
path = "/var/lib/triplox/data/log"

[server]
host = "0.0.0.0"
port = 5490
```

Set `server.host` to `0.0.0.0` when publishing the server port from a
container. If it is omitted, Triplox binds to `127.0.0.1` inside the container
and cannot be reached through Docker's published port. Custom remote
configurations contain credentials and should be mounted read-only or supplied
as a container secret.

#### Tuning knobs

[SlateDB](https://slatedb.io/docs/operations/tuning/) has a lot of tuning knobs
(see also the [SlateDB benchmarks](https://benchmark.slatedb.io/0.16.0/workload/balanced/).
Before dumping all these options onto the user I'd like to spend some more time
to figure out what some sensible defaults for Triplox are. At some point we likely
expose more and more knobs for the user to tune.


### Pricing

I admit that I have not done a lot of testing in the Cloud so far. The pricing highly
depends on the object storage provider, throughput and latency requirements, ingestion
workloads and flushing intervals to just name a couple of factors. SlateDB has some
numbers on their [benchmark pages](https://benchmark.slatedb.io/0.16.0/workload/balanced/).
To really get an accurate number, benchmark your workload.
