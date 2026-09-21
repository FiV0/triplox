# Triplox Configs

TOML configs for running triplox directly via `cargo run`. Pick a file that
matches the storage backend you want and pass its path as the first argument:

```bash
cargo run -- config/triplox.toml
```

If no argument is passed, triplox loads `config/triplox.toml` by default (see
`src/main.rs`).

Storage and log are configured independently via the `[storage]` and `[log]`
sections. The supported combinations are:

| Mode   | `[storage].type` | `[log].type` |
|--------|------------------|--------------|
| dev    | `dev`            | *(ignored)*  |
| memory | `memory`         | `memory`     |
| local  | `local`          | `file`       |
| remote | `remote`         | `file`       |
| kafka  | `remote`         | `kafka`      |

`dev` runs a fresh in-memory node per connection and ignores `[log]`; every
other mode requires the matching `[log]` shown above. Any other (log, storage)
combination is rejected at startup.

| File                  | Mode   | Notes                                             |
|-----------------------|--------|---------------------------------------------------|
| `triplox.toml`        | memory | In-process, no persistence. Default.              |
| `triplox-dev.toml`    | dev    | Dev-only: a fresh in-memory node per connection.  |
| `triplox-local.toml`  | local  | Persistent local FS at `./data/`.                 |
| `triplox-remote.toml` | remote | S3-compatible (MinIO) at `http://localhost:9000`. |

## Writer WAL flush interval

For remote storage, set `wal_flush_interval_us` in `[storage]` to control how
often the writer flushes SlateDB's WAL to object storage. The value is a positive
integer in microseconds and defaults to `100`, preserving the existing interval.
It applies with both file and Kafka transaction logs.

```toml
# Add to the existing [storage] section with type = "remote":
wal_flush_interval_us = 25000 # 25 milliseconds
```

Longer intervals allow more writes to accumulate between flushes, but can delay
visibility to readers and incremental queries. This setting controls SlateDB's
WAL, not the transaction log configured in `[log]`. Local and memory storage
continue to use SlateDB's default interval.

## Running locally against MinIO in Docker

Useful when you want to exercise the remote-storage code path while iterating
on triplox natively. MinIO stays in Docker; triplox runs from `cargo`.

### 1. One-time ext4 loopback setup

MinIO refuses to start on btrfs, so `docker/data/minio/` is a loopback ext4
image. Create and mount it once (and re-run after each reboot):

```bash
./docker/scripts/setup-minio-disk.sh
```

### 2. Start only MinIO (not triplox) in Docker

```bash
docker compose -f docker/docker-compose.yml up minio createbucket
```

`createbucket` exits after creating the `triplox` bucket; `minio` keeps
running on `localhost:9000` (S3 API) and `localhost:9001` (console).

### 3. Run triplox locally

```bash
cargo run -- config/triplox-remote.toml              # debug build
cargo run --release -- config/triplox-remote.toml    # release build
```

The local transaction log is `/tmp/triplox-log/log` (configured as `[log].path`
in `triplox-remote.toml`). SlateDB's disk-backed object-store cache lives at
`/tmp/triplox-disk/cache/`, and DBSP incremental query storage lives at
`/tmp/triplox-disk/dbsp/` (both derived from `[storage].cache_path`). The
SlateDB cache is capped at SlateDB's default 16 GiB. It grows across restarts
and must be wiped manually when you want a cold read path.

### 4. Reset

Wipe MinIO contents, the local log, and local disk storage before the next run:

```bash
./config/scripts/reset-local-remote.sh
```
