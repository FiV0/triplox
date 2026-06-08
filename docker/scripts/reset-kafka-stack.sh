#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

COMPOSE_FILE="$REPO_ROOT/docker/docker-compose-kafka.yml"
MINIO_MNT="$REPO_ROOT/docker/data/minio"

wipe_minio_data() {
    local minio_dir="$1"
    local find_args=(-mindepth 1 -maxdepth 1)

    if [ "$2" = "preserve-lost-found" ]; then
        find_args+=(! -name 'lost+found')
    fi

    if ! find "$minio_dir" "${find_args[@]}" -exec rm -rf {} +; then
        echo "Host wipe failed; retrying with a root container for root-owned MinIO files"
        docker run --rm -v "$minio_dir:/data" busybox:1.36 sh -c '
            if [ "$1" = "preserve-lost-found" ]; then
                for path in /data/..?* /data/.[!.]* /data/*; do
                    [ -e "$path" ] || continue
                    [ "$(basename "$path")" = "lost+found" ] && continue
                    rm -rf "$path"
                done
            else
                rm -rf /data/..?* /data/.[!.]* /data/*
            fi
        ' sh "$2"
    fi
}

echo "Stopping Kafka stack and removing named volumes"
docker compose -f "$COMPOSE_FILE" down -v

if mountpoint -q "$MINIO_MNT"; then
    echo "Wiping minio data under $MINIO_MNT (loopback mount)"
    wipe_minio_data "$MINIO_MNT" preserve-lost-found
elif [ -d "$MINIO_MNT" ]; then
    echo "Wiping minio data under $MINIO_MNT"
    wipe_minio_data "$MINIO_MNT"
else
    echo "Skipping minio wipe - $MINIO_MNT does not exist"
fi

echo "Done. Bring the Kafka stack back up with:"
echo "  docker compose -f docker/docker-compose-kafka.yml up --build"
