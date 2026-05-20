# Release Process

Triplox releases use one version for the Rust workspace, Docker image, JVM
client, and GitHub Release. Release tags use `vX.Y.Z`, `vX.Y.Z-alpha.N`, or
`vX.Y.Z-beta.N`; the package version omits the `v` prefix.

## Prepare a release

Run from a clean branch:

```bash
./scripts/release.sh 0.1.0-alpha.2
```

The script validates the version, updates Cargo versions, refreshes
`Cargo.lock`, runs formatting/tests/clippy, creates a release commit, and adds
an annotated tag.

Review the generated commit, then publish:

```bash
git push origin main
git push origin v0.1.0-alpha.2
```

Pushing the tag triggers GitHub Actions to publish the Rust crates, JVM client,
Docker image, and GitHub Release. The JVM client is published to Maven Central
after Central Portal validation as `xyz.triplox:triplox`.

To publish only the Docker image for an existing release tag, run the
`Docker Publish` workflow manually, select the release tag as the ref, and set
`mode` to `release`. To publish a SNAPSHOT Docker image from a branch, run the
same workflow on the branch ref with `mode` set to `snapshot`.

## Required GitHub variables and secrets

- `CARGO_REGISTRY_TOKEN` for crates.io.
- `CENTRAL_USERNAME` as a repository variable.
- `CENTRAL_PASSWORD` for Maven Central.
- `SIGNING_KEY` as an ASCII-armored GPG private key for Maven Central.
- `SIGNING_PASSWORD` for the GPG private key.

`CENTRAL_USERNAME` and `CENTRAL_PASSWORD` are the Sonatype Central Portal user
token values, not the account login password. Generate them from the Central
Portal account settings.
