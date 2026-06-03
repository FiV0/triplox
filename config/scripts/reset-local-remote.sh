#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

"$REPO_ROOT/docker/scripts/reset-remote-stack.sh"

TRIPLOX_LOG="/tmp/triplox-log"
TRIPLOX_DISK="/tmp/triplox-disk"
echo "Wiping $TRIPLOX_LOG (triplox file log)"
rm -rf "$TRIPLOX_LOG"
echo "Wiping $TRIPLOX_DISK (SlateDB cache + DBSP storage)"
rm -rf "$TRIPLOX_DISK"
