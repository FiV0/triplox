---
title: Operations
description: Operating Triplox in production.
---

## Deployment

:::note
Triplox currently runs as a single writer node. Production deployment guidance
will evolve as additional node roles and cloud integrations are added.
:::

Triplox images are published to the
[GitHub Container Registry](https://github.com/FiV0/triplox/pkgs/container/triplox).
See the [Quick Start](/getting-started/quick-start/) for an introduction.

The default image starts an in-memory database on port 5490:

```bash
docker run --rm -p 5490:5490 ghcr.io/fiv0/triplox:0.1.0-alpha.8
```

### Configuration selection

The image selects its configuration in the following order:

| Priority | Mechanism | Example |
|---|---|---|
| 1 | Explicit container argument | `/config/triplox.toml` |
| 2 | `TRIPLOX_CONFIG_FILE` | `/config/triplox.toml` |
| 3 | `TRIPLOX_STORAGE` | `local`, `remote`, or `kafka` |
| 4 | Image default | `/etc/triplox/config.toml` |

An explicit argument is passed directly to the Triplox binary and takes
precedence over all environment variables.

`TRIPLOX_CONFIG_FILE` is the recommended way to select a custom configuration.
`TRIPLOX_STORAGE` selects one of the configurations bundled with the image; it
does not override individual fields.

When none of these options is provided, Triplox uses
`/etc/triplox/config.toml`, which contains the in-memory configuration.

### Bundled storage modes

| Mode | Database storage | Transaction log | Persistence |
|---|---|---|---|
| `dev` | In memory | In memory | A fresh database is created for every connection. |
| `memory` | In memory | In memory | Data is lost when the container stops. |
| `local` | Local filesystem | Local file | Data is stored under `/var/lib/triplox`. |
| `remote` | Object storage | Local file | Object storage is remote; the transaction log and cache are local. |
| `kafka` | Object storage | Kafka | Durable database state and the transaction log are external. |

For example, start a persistent local node with:

```bash
docker run --rm \
  -p 5490:5490 \
  -e TRIPLOX_STORAGE=local \
  -v triplox-data:/var/lib/triplox \
  ghcr.io/fiv0/triplox:0.1.0-alpha.8
```

### Remote and Kafka presets

The bundled `remote` and `kafka` presets use role-based hostnames on the
container network:

| Mode | Object storage | Kafka |
|---|---|---|
| `remote` | `object-storage:9000`, bucket `triplox` | — |
| `kafka` | `object-storage:9000`, bucket `triplox-kafka` | `kafka:9092`, topic `triplox-tx-log` |

The provided Compose files assign these aliases to MinIO and AutoMQ:

```bash
docker compose -f docker/docker-compose.yml up --build
docker compose -f docker/docker-compose-kafka.yml up --build
```

The bundled endpoints and credentials are intended for these development
stacks. Production deployments should supply a custom configuration containing
their actual endpoints, credentials, buckets, and topics.

### Custom configuration

Mount the configuration at a convenient location and point
`TRIPLOX_CONFIG_FILE` to it:

```bash
docker run --rm \
  -p 5490:5490 \
  --mount type=bind,src="$(pwd)/triplox.toml",dst=/config/triplox.toml,readonly \
  --mount type=volume,src=triplox-data,dst=/var/lib/triplox \
  -e TRIPLOX_CONFIG_FILE=/config/triplox.toml \
  ghcr.io/fiv0/triplox:0.1.0-alpha.8
```

The same file can instead be supplied as the explicit image argument:

```bash
docker run --rm \
  -p 5490:5490 \
  --mount type=bind,src="$(pwd)/triplox.toml",dst=/config/triplox.toml,readonly \
  ghcr.io/fiv0/triplox:0.1.0-alpha.8 \
  /config/triplox.toml
```

Mounting a file directly over `/etc/triplox/config.toml` also works, but using
`TRIPLOX_CONFIG_FILE` avoids coupling the deployment to the image's fallback
path.

A persistent local configuration looks like this:

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
configurations contain credentials and should be mounted read-only and handled
as secrets.

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
