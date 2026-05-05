# Development

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

### JVM client (Clojars)

From `triplox-jvm/`:

Alpha release:

```bash
./gradlew publishMavenPublicationToClojarsRepository
```

Alpha SNAPSHOT:

```bash
./gradlew publishMavenPublicationToClojarsRepository \
    -Dorg.gradle.internal.publish.checksums.insecure=true \
    -PtriploxVersion=0.1.0-alpha-SNAPSHOT
```

See [triplox-jvm/README.md](triplox-jvm/README.md) for running the JVM
client's unit and integration tests.
