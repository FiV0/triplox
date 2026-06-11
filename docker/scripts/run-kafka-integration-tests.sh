#!/usr/bin/env bash
set -euo pipefail

# Starts the AutoMQ Kafka stack, runs the kafka-integration-test suite against
# it from the host, and tears the stack down. Used locally and by CI.

cd "$(dirname "$0")/../.."

export KAFKA_ADVERTISED_HOST=localhost
PROJECT=triplox-kafka-test
COMPOSE=(docker compose
  -f docker/docker-compose-kafka.yml
  -f docker/docker-compose-kafka.test.yml
  -p "$PROJECT")

cleanup() {
  "${COMPOSE[@]}" down -v
}
trap cleanup EXIT

"${COMPOSE[@]}" up -d automq

echo "Waiting for the AutoMQ broker to become healthy..."
status=starting
for _ in $(seq 1 36); do
  status=$(docker inspect -f '{{.State.Health.Status}}' "$PROJECT-automq-1" 2>/dev/null || echo starting)
  [ "$status" = healthy ] && break
  sleep 5
done
if [ "$status" != healthy ]; then
  echo "AutoMQ broker did not become healthy" >&2
  "${COMPOSE[@]}" logs automq | tail -50 >&2
  exit 1
fi

KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  cargo test -p triplox --features kafka-integration-test kafka_log::tests
