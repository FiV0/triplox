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

To deploy the current workspace version to Clojars:

```bash
./gradlew publishMavenPublicationToClojarsRepository
```

To override the published Clojars version:

```bash
./gradlew publishMavenPublicationToClojarsRepository \
    -Dorg.gradle.internal.publish.checksums.insecure=true \
    -PtriploxVersion=0.1.0-alpha.2
```

To upload the current workspace version to Maven Central:

```bash
CENTRAL_USERNAME=... \
CENTRAL_PASSWORD=... \
SIGNING_KEY="$(gpg --armor --export-secret-keys <key-id>)" \
SIGNING_PASSWORD=... \
./gradlew publishMavenPublicationToCentralPortal
```

To override the Maven Central version:

```bash
CENTRAL_USERNAME=... \
CENTRAL_PASSWORD=... \
SIGNING_KEY="$(gpg --armor --export-secret-keys <key-id>)" \
SIGNING_PASSWORD=... \
./gradlew publishMavenPublicationToCentralPortal \
    -PtriploxVersion=0.1.0-alpha.2
```

The Maven Central task uploads the deployment in `user_managed` mode by default.
After it succeeds, inspect the deployment at <https://central.sonatype.com/publishing>
and click Publish.
