# Plan: Main Entrypoint with TOML Config

## Context

The TCP client-server was recently added (commit 3e5cfe5e5) but `src/main.rs` is still a "Hello, world!" stub. We need a real entrypoint that reads a config file, creates the appropriate Node (memory/local/remote), and starts the server with graceful shutdown.

**Format choice: TOML** — idiomatic Rust (Cargo.toml), well-typed (no YAML Norway problem / implicit coercion), excellent serde support via the `toml` crate. EDN support deferred to a separate ticket.

## Proposed TOML Config Format

```toml
# Memory (dev/testing)
[storage]
type = "memory"

[server]
host = "127.0.0.1"
port = 5490
max_open_dbs = 1024
```

```toml
# Local (single-machine)
[storage]
type = "local"
path = "/var/lib/triplox/data"

[server]
host = "0.0.0.0"
port = 5490
```

```toml
# Remote (parses but errors at startup)
[storage]
type = "remote"
bucket = "s3://my-bucket"
region = "us-east-1"
```

All `[server]` fields are optional with defaults: host=`127.0.0.1`, port=`5490`, max_open_dbs=`1024`. The entire `[server]` section can be omitted.

## Changes

### 1. Add `toml` dependency — `Cargo.toml`
```
toml = "0.8"
```

### 2. New file: `src/config.rs`

Config structs with serde derives:

- **`Config`** — top-level with `storage: StorageConfig` and `server: ServerConfig` (defaulted)
- **`StorageConfig`** — `#[serde(tag = "type", rename_all = "lowercase")]` enum: `Memory`, `Local { path: PathBuf }`, `Remote { #[serde(flatten)] _extra: toml::Table }`
- **`ServerConfig`** — with `Default` impl providing host/port/max_open_dbs defaults, plus `bind_addr() -> String` helper

### 3. Modify `src/lib.rs`

- Add `pub mod config;`
- Change `mod logging` → `pub mod logging` (main.rs needs `triplox::logging::init()`)

### 4. Replace `src/main.rs`

```
1. triplox::logging::init()
2. load_config():
   - Config path from first CLI arg, default "triplox.toml"
   - std::fs::read_to_string → toml::from_str
   - anyhow::Context for clear error messages
3. Match on StorageConfig:
   - Memory  → Node::memory_node().await
   - Local   → Node::local_node(&path).await
   - Remote  → bail!("Remote storage is not yet supported")
4. Server::new(node, max_open_dbs)
5. Set up Ctrl+C → CancellationToken (tokio::signal::ctrl_c)
6. server.listen(&bind_addr, token).await
```

Note: `Node<MemoryLog>` and `Node<FileLog>` are different concrete types, so each match arm has its own server creation. The duplication is minimal (3 lines each).

### 5. Create `triplox.toml` — default config file at project root

Memory config as a working default / documentation.

### 6. Create beads ticket for EDN config support

### 7. Add cargo-watch dev run configuration

Install `cargo-watch` (if not already available) and add a convenience script/alias for dev workflow:

```bash
cargo watch -x 'run -- triplox.toml'
```

This auto-restarts the server on source changes. We'll document the command and optionally add it as a `cargo-xtask` or `.cargo/config.toml` alias so `cargo dev` works.

## File Summary

| File | Action |
|------|--------|
| `Cargo.toml` | Add `toml = "0.8"` |
| `src/config.rs` | **New** — Config, StorageConfig, ServerConfig |
| `src/lib.rs` | Add `pub mod config`, make `logging` pub |
| `src/main.rs` | **Replace** — full server entrypoint |
| `triplox.toml` | **New** — default config example |
| `.cargo/config.toml` | **New** — alias `cargo dev` → `cargo watch -x 'run -- triplox.toml'` |

## Verification

1. `cargo build` — compiles without errors
2. `cargo run` — reads `triplox.toml`, starts memory server on 127.0.0.1:5490
3. `cargo run -- path/to/custom.toml` — reads custom config
4. Test with local config: create a local storage toml, verify server starts and data dir is created
5. Test remote config: verify it parses but exits with "not yet supported" error
6. `cargo test` — existing tests still pass (changes are additive)
7. `cargo dev` — starts server with file watching, auto-restarts on changes
