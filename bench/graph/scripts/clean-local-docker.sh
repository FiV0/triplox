#!/usr/bin/env bash
set -euo pipefail

VOLUME_NAME="${TRIPLOX_DOCKER_VOLUME:-triplox-data}"

if ! docker volume inspect "$VOLUME_NAME" >/dev/null 2>&1; then
    echo "Docker volume $VOLUME_NAME does not exist."
    exit 0
fi

mapfile -t containers < <(docker ps -aq --filter "volume=$VOLUME_NAME")

if [ "${#containers[@]}" -gt 0 ]; then
    echo "Removing containers attached to $VOLUME_NAME:"
    docker ps -a --filter "volume=$VOLUME_NAME" --format '  {{.ID}} {{.Image}} {{.Status}} {{.Names}}'
    docker rm -f "${containers[@]}"
else
    echo "No containers attached to $VOLUME_NAME."
fi

echo "Removing Docker volume $VOLUME_NAME"
docker volume rm "$VOLUME_NAME"
