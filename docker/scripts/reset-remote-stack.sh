#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

COMPOSE_FILE="$REPO_ROOT/docker/docker-compose.yml"

echo "Stopping stack and removing named volumes (rustfs-data and triplox-log)"
docker compose -f "$COMPOSE_FILE" down -v

echo "Done. Bring the stack back up with:"
echo "  docker compose -f docker/docker-compose.yml up --build"
