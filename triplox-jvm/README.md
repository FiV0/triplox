# triplox-jvm

Java and Clojure client for Triplox.

## Running tests

Unit tests (no server required):

```bash
./gradlew test
```

## Running integration tests

Integration tests require a running Triplox dev server.

1. Start the dev server from the repository root:

```bash
cargo run -- config/triplox-dev.toml
```

2. Run the integration tests:

```bash
cd triplox-jvm
./gradlew integrationTest
```

By default the tests connect to `localhost:5490`. Override with environment variables:

```bash
TRIPLOX_HOST=192.168.1.10 TRIPLOX_PORT=5491 ./gradlew integrationTest
```

## Deploying

To upload the current workspace version to Maven Central:

```bash
ORG_GRADLE_PROJECT_mavenCentralUsername=<central-token-username> \
ORG_GRADLE_PROJECT_mavenCentralPassword=<central-token-password> \
ORG_GRADLE_PROJECT_signAllPublications=true \
ORG_GRADLE_PROJECT_signingInMemoryKey="$(gpg --armor --export-secret-keys <key-id>)" \
ORG_GRADLE_PROJECT_signingInMemoryKeyPassword=<gpg-key-passphrase> \
./gradlew publishToMavenCentral
```

To override the Maven Central version:

```bash
ORG_GRADLE_PROJECT_mavenCentralUsername=<central-token-username> \
ORG_GRADLE_PROJECT_mavenCentralPassword=<central-token-password> \
ORG_GRADLE_PROJECT_signAllPublications=true \
ORG_GRADLE_PROJECT_signingInMemoryKey="$(gpg --armor --export-secret-keys <key-id>)" \
ORG_GRADLE_PROJECT_signingInMemoryKeyPassword=<gpg-key-passphrase> \
./gradlew publishToMavenCentral \
    -PtriploxVersion=0.1.0-alpha.2
```

The Maven Central username and password are Central Portal user token values.
The signing key is the ASCII-armored private GPG key, and the matching public
key must be available on a public keyserver before release validation succeeds.

Maven Central uses the `xyz.triplox:triplox` coordinate.

The Maven Central task uploads the deployment for manual publishing by default.
After it succeeds, inspect the deployment at <https://central.sonatype.com/publishing>
and click Publish. Use `./gradlew publishAndReleaseToMavenCentral` when you want
the plugin to publish automatically after Central Portal validation. Tagged
GitHub releases use the automatic task.

If `signAllPublications=true` is set in `~/.gradle/gradle.properties`, local
non-release checks such as `publishMavenPublicationToMavenLocal` also expect a
signing key. Pass `-PsignAllPublications=false` for those checks when you are
not testing signing.

To publish locally for testing, you can do:
```bash
./gradlew publishMavenPublicationToMavenLocal
    -PtriploxVersion=0.1.0-SNAPSHOT
    -PsignAllPublications=false
```
