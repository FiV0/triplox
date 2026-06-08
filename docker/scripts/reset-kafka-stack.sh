#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

COMPOSE_FILE="$REPO_ROOT/docker/docker-compose-kafka.yml"
MINIO_MNT="$REPO_ROOT/docker/data/minio"

echo "Stopping Kafka stack and removing named volumes"
docker compose -f "$COMPOSE_FILE" down -v

if mountpoint -q "$MINIO_MNT"; then
    echo "Wiping minio data under $MINIO_MNT (loopback mount)"
    find "$MINIO_MNT" -mindepth 1 -maxdepth 1 ! -name 'lost+found' -exec rm -rf {} +
elif [ -d "$MINIO_MNT" ]; then
    echo "Wiping minio data under $MINIO_MNT"
    find "$MINIO_MNT" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
else
    echo "Skipping minio wipe - $MINIO_MNT does not exist"
fi

echo "Done. Bring the Kafka stack back up with:"
echo "  docker compose -f docker/docker-compose-kafka.yml up --build"
