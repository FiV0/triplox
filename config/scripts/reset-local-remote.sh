#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

"$REPO_ROOT/docker/scripts/reset-remote-stack.sh"

TRIPLOX_LOG="/tmp/triplox-log"
echo "Wiping $TRIPLOX_LOG (triplox log + object-store cache)"
rm -rf "$TRIPLOX_LOG"
