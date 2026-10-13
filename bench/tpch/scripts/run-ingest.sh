#!/bin/bash
# Time a TPC-H ingest against a release build of this checkout: SlateDB on RustFS, local file log.
# Usage: SCALE_FACTOR=1 ./bench/tpch/scripts/run-ingest.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TPCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$TPCH_DIR/../.." && pwd)"

SCALE_FACTOR="${SCALE_FACTOR:-1}"
PORT="${TRIPLOX_PORT:-5490}"
RUN_DIR="$TPCH_DIR/.ingest"
COMPOSE=(docker compose -p tpch-ingest -f "$TPCH_DIR/docker-compose.yml")
CLIENT_VERSION="0.1.0-local-SNAPSHOT"

SERVER_PID=""
cleanup() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Building triplox (release)..."
cargo build --release --bin triplox --manifest-path "$REPO_ROOT/Cargo.toml"

echo "Publishing JVM client $CLIENT_VERSION to Maven local..."
(cd "$REPO_ROOT/triplox-jvm" && ./gradlew -q publishToMavenLocal -PtriploxVersion="$CLIENT_VERSION")

echo "Starting RustFS..."
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --wait rustfs
"${COMPOSE[@]}" run --rm createbucket

rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR"
cat > "$RUN_DIR/triplox.toml" <<EOF
[storage]
type = "remote"
endpoint = "http://localhost:9000"
bucket = "triplox"
access_key = "triplox"
secret_key = "triplox123"
cache_path = "$RUN_DIR/disk"

[log]
type = "file"
path = "$RUN_DIR/log/log"

[server]
host = "127.0.0.1"
port = $PORT
EOF

echo "Starting triplox (server log: $RUN_DIR/server.log)..."
"$REPO_ROOT/target/release/triplox" "$RUN_DIR/triplox.toml" >"$RUN_DIR/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 60); do
    if (echo >"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then break; fi
    kill -0 "$SERVER_PID" 2>/dev/null || { echo "triplox exited, see $RUN_DIR/server.log"; exit 1; }
    sleep 1
done

cd "$TPCH_DIR"
time TRIPLOX_HOST=localhost TRIPLOX_PORT="$PORT" SCALE_FACTOR="$SCALE_FACTOR" \
    clojure -M:local:ingest

du -sh "$RUN_DIR/log" "$RUN_DIR/disk" 2>/dev/null || true
