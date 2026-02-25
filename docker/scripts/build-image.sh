#!/bin/bash
set -e

# Build from the repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"

docker build \
    -f "$REPO_ROOT/docker/Dockerfile" \
    --build-arg GIT_SHA="$GIT_SHA" \
    -t "triplox:latest" \
    -t "triplox:$GIT_SHA" \
    "$REPO_ROOT"

echo "Built triplox:latest and triplox:$GIT_SHA"
