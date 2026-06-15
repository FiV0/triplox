# Triplox Docker

Docker support for running Triplox with in-memory, local persisted, or remote S3-compatible storage.

## Building

For local development:

```bash
./docker/scripts/build-image.sh
```

This builds and tags the image as `triplox:latest` and `triplox:<short-sha>`.

## Published images

Release images are published to GitHub Container Registry:

```bash
docker pull ghcr.io/fiv0/triplox:0.1.0-alpha.2
```

Images are published only from release tags matching `vX.Y.Z`,
`vX.Y.Z-alpha.N`, or `vX.Y.Z-beta.N`. The tag version must match the Cargo
workspace version in the tagged commit.

Published tags:

- `X.Y.Z-alpha.N` or `X.Y.Z-beta.N` for prereleases.
- `X.Y.Z` and `X.Y` for stable releases.
- `X.Y.Z-snapshot` and `snapshot` for manual SNAPSHOT builds.
- `sha-<short-sha>` for traceability.

Docker publishing runs as part of the release workflow. To publish only the
Docker image for an existing release tag, run the `Docker Publish` workflow
manually in GitHub Actions, select the release tag as the workflow ref, and set
`mode` to `release`. To publish a SNAPSHOT image from a branch, select the
branch ref and set `mode` to `snapshot`.

## Running

### In-memory mode (default)

```bash
docker run -p 5490:5490 triplox:latest
```

### Local persisted mode

```bash
docker run -p 5490:5490 \
  -e TRIPLOX_STORAGE=local \
  -v triplox-data:/var/lib/triplox \
  triplox:latest
```

### Remote mode (S3-compatible with MinIO)

#### First-time setup

MinIO data lives on a loopback ext4 image on disk (MinIO's sanity check treats
btrfs as corrupt, so a plain bind mount to a btrfs host FS won't work). Create
and mount the image once, then re-run after each reboot:

```bash
./docker/scripts/setup-minio-disk.sh
```

This creates a 50G sparse image at `docker/data/minio.img`, formats it ext4,
and mounts it at `docker/data/minio/`. Override the size with
`MINIO_IMG_SIZE=100G ./docker/scripts/setup-minio-disk.sh`.

To unmount later: `sudo umount docker/data/minio`.

#### Starting the stack

Using docker-compose:

```bash
docker compose -f docker/docker-compose.yml up --build
```

This starts:
- **MinIO** on port 9000 (S3 API) and 9001 (console)
- **Triplox** on port 5490 with SlateDB backed by MinIO

#### Tracing object-store operations

Stream real-time S3 calls against MinIO from the host:

```bash
docker compose -f docker/docker-compose.yml exec mc mc admin trace -v triplox
```

Useful filters: `--call s3`, `--status-code 4xx,5xx`, `--funcname s3.GetObject`.

#### Resetting the stack

Wipe all MinIO data and the triplox local log, and bring everything back to a
clean slate:

```bash
./docker/scripts/reset-remote-stack.sh
```

This runs `docker compose down -v` (removing the `triplox-log` named volume)
and clears the contents of `docker/data/minio/` (keeping the ext4 mount). It
does not unmount or delete the loopback image. After it finishes, re-run
`docker compose -f docker/docker-compose.yml up --build`.

### Kafka mode (AutoMQ)

Using docker-compose with AutoMQ as the Kafka-compatible broker backed by S3:

```bash
docker compose -f docker/docker-compose-kafka.yml up --build
```

This starts:
- **MinIO** on port 9000 (S3 API) and 9001 (console)
- **AutoMQ** (Kafka-compatible broker) on port 9092, backed by MinIO
- **Triplox** on port 5490 with transaction log on Kafka and SlateDB on MinIO

The Kafka topic (`triplox-tx-log`) uses a single partition to guarantee total
ordering (WAL semantics), and `message.timestamp.type=LogAppendTime` so Triplox
transaction times come from the broker append time rather than producer clocks.

#### Running the Kafka integration tests

The `kafka-integration-test` cargo feature gates tests that need a live broker;
without `KAFKA_BOOTSTRAP_SERVERS` set they skip silently. Run them against the
AutoMQ stack with:

```bash
./docker/scripts/run-kafka-integration-tests.sh
```

The script starts MinIO + AutoMQ (advertising `localhost:9092` so host clients
can connect, with MinIO data on a named volume instead of the loopback mount),
runs `cargo test -p triplox --features kafka-integration-test kafka_log::tests`,
and tears the stack down afterwards. The same script backs the manually
triggered `Kafka Integration` workflow (`gh workflow run kafka-integration.yml`
or the Actions UI). Each test creates its own uniquely named topic,
so runs don't interfere with a concurrently running dev stack's data — though
both bind port 9092, so stop the dev stack first.

#### Resetting the Kafka stack

Wipe all MinIO data used by the Kafka stack and remove Kafka compose named
volumes:

```bash
./docker/scripts/reset-kafka-stack.sh
```

This stops `docker/docker-compose-kafka.yml`, removes named volumes such as
`mc-config`, and clears the contents of `docker/data/minio/` (keeping the ext4
mount). It resets the AutoMQ buckets (`automq-data`, `automq-ops`) and the
Triplox Kafka storage bucket (`triplox-kafka`). Because MinIO data is shared
with the remote stack, it also removes any remote-stack bucket data in that
directory. After it finishes, re-run
`docker compose -f docker/docker-compose-kafka.yml up --build`.

### Custom config

Mount your own config file and pass its path as an argument:

```bash
docker run -p 5490:5490 \
  -v ./my-config.toml:/etc/triplox/custom.toml \
  triplox:latest /etc/triplox/custom.toml
```

Config files use independent `[storage]` and `[log]` sections (see
`docker/config/` for examples). Only the pairings (memory, memory),
(file, local), (file, remote), and (kafka, remote) are supported — any other
combination is rejected at startup. The dev server uses a top-level
`dev = true` flag instead.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TRIPLOX_STORAGE` | `memory` | Storage mode: `dev`, `memory`, `local`, `remote`, or `kafka` |

## Ports

| Port | Protocol | Description |
|---|---|---|
| 5490 | TCP | Triplox wire protocol |
| 9092 | TCP | Kafka broker (AutoMQ, kafka mode only) |

## Volumes

| Path | Description |
|---|---|
| `/var/lib/triplox` | Persistent data directory (used in `local` mode) |
