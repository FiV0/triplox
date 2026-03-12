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
