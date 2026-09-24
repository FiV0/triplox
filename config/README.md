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
| `triplox-remote.toml` | remote | S3-compatible (RustFS) at `http://localhost:9000`. |

## Running locally against RustFS in Docker

Useful when you want to exercise the remote-storage code path while iterating
on triplox natively. RustFS stays in Docker; triplox runs from `cargo`.

### 1. Start RustFS in Docker

Objects live in the Compose-managed `rustfs-data` volume; no host disk setup
is needed.

```bash
docker compose -f docker/docker-compose.yml up rustfs createbucket
```

`createbucket` exits after creating the `triplox` bucket; `rustfs` keeps
running on `localhost:9000` (S3 API) and `localhost:9001` (console).

### 2. Run triplox locally

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

### 3. Reset

Wipe RustFS contents, the local log, and local disk storage before the next run:

```bash
./config/scripts/reset-local-remote.sh
```
