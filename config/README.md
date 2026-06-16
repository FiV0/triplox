# Triplox Configs

TOML configs for running triplox directly via `cargo run`. Pick a file that
matches the storage backend you want and pass its path as the first argument:

```bash
cargo run -- config/triplox.toml
```

If no argument is passed, triplox loads `config/triplox.toml` by default (see
`src/main.rs`).

Storage and log are configured independently via the `[storage]` and `[log]`
sections. Only four (log, storage) pairings are supported; any other
combination is rejected at startup:

| Mode   | `[storage].type` | `[log].type` |
|--------|------------------|--------------|
| memory | `memory`         | `memory`     |
| local  | `local`          | `file`       |
| remote | `remote`         | `file`       |
| kafka  | `remote`         | `kafka`      |

The dev server (a fresh in-memory node per connection) is selected with the
top-level `type = "dev"` setting instead of a `[storage]`/`[log]` pair.

| File                  | Mode           | Notes                                             |
|-----------------------|----------------|---------------------------------------------------|
| `triplox.toml`        | memory         | In-process, no persistence. Default.              |
| `triplox-dev.toml`    | `type = "dev"` | Dev-only: a fresh in-memory node per connection.  |
| `triplox-local.toml`  | local          | Persistent local FS at `./data/`.                 |
| `triplox-remote.toml` | remote         | S3-compatible (MinIO) at `http://localhost:9000`. |

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
