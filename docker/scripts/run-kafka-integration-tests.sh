#!/usr/bin/env bash
set -euo pipefail

# Starts the AutoMQ Kafka stack, runs the kafka-integration-test suite against
# it from the host, and tears the stack down. Used locally and by CI.

cd "$(dirname "$0")/../.."

export KAFKA_ADVERTISED_HOST=localhost
COMPOSE=(docker compose
  -f docker/docker-compose-kafka.yml
  -f docker/docker-compose-kafka.test.yml
  -p triplox-kafka-test)

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "Failed with exit status $status; dumping broker logs:" >&2
    "${COMPOSE[@]}" logs automq 2>/dev/null | tail -100 >&2 || true
  fi
  "${COMPOSE[@]}" down -v
}
trap cleanup EXIT

echo "Starting the stack and waiting for the AutoMQ broker to become healthy..."
"${COMPOSE[@]}" up -d --wait --wait-timeout 180 automq

KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  cargo test -p triplox --features kafka-integration-test kafka_log::tests
