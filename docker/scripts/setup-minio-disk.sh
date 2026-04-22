#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

IMG="$REPO_ROOT/docker/data/minio.img"
MNT="$REPO_ROOT/docker/data/minio"
SIZE="${MINIO_IMG_SIZE:-50G}"

mkdir -p "$(dirname "$IMG")"

if [ ! -e "$IMG" ]; then
    echo "Creating $SIZE sparse ext4 image at $IMG"
    truncate -s "$SIZE" "$IMG"
    mkfs.ext4 -q "$IMG"
fi

mkdir -p "$MNT"

if mountpoint -q "$MNT"; then
    echo "$MNT already mounted"
    df -h "$MNT" | tail -n 1
    exit 0
fi

echo "Mounting $IMG at $MNT (sudo required)"
sudo mount -o loop "$IMG" "$MNT"
sudo chown "$(id -u):$(id -g)" "$MNT"
chmod 1777 "$MNT"

df -h "$MNT" | tail -n 1
