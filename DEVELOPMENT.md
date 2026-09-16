# Development

## Rust build cache

Optionally install sccache to reuse compiled dependencies across worktrees:

```bash
cargo install sccache --locked
```

Add this setting to your user Cargo configuration, `~/.cargo/config.toml`
(merge it into an existing `[build]` section if present):

```toml
[build]
rustc-wrapper = "sccache"
```

This applies to all your Cargo projects, including existing and new worktrees.
Each worktree keeps its own `target/` directory, and workspace crates retain
Cargo's default incremental compilation settings. The cache warms as you
build; inspect it with `sccache --show-stats`.

To bypass your wrapper setting for a build, use `RUSTC_WRAPPER= cargo build`.
Sccache is not required to build the repository.

## Current versions

| Crate / artifact | Version | Source |
| --- | --- | --- |
| All Rust crates (`triplox`, `triplox-edn`, `triplox-client`) | `0.1.0-alpha.1` | `[workspace.package].version` in root `Cargo.toml` |
| `triplox-jvm` | `0.1.0-alpha` (default, override with `-PtriploxVersion`) | `triplox-jvm/build.gradle.kts` |

Rust crates move in lockstep — bump `[workspace.package].version` in the root
`Cargo.toml`, plus the matching `version =` in each `[workspace.dependencies]`
entry that has one (e.g. `edn`, `triplox-client`). Cargo doesn't currently
allow `version.workspace = true` inside `[workspace.dependencies]`, so those
two strings still need updating per release.

## Deploying

### Rust crates (crates.io)

Two workspace members are publishable:

- `triplox-edn` (in `edn/`)
- `triplox-client` (in `triplox-client/`), depends on `triplox-edn`

```bash
# Sanity-check the tarballs
cargo publish -p triplox-edn --dry-run
cargo publish -p triplox-client --dry-run

# Publish for real
cargo publish -p triplox-edn
# wait ~30s for the index to update
cargo publish -p triplox-client
```

### JVM client (Maven Central)

From `triplox-jvm/`:

See [triplox-jvm/README.md](triplox-jvm/README.md) for Maven Central
publishing commands, credentials, and local JVM client tests.
