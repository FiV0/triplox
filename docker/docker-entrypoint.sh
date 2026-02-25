#!/bin/bash
set -e

# If arguments are passed, use them as the config path
if [ $# -gt 0 ]; then
    exec triplox "$@"
fi

# Select config based on TRIPLOX_STORAGE env var
TRIPLOX_STORAGE="${TRIPLOX_STORAGE:-memory}"

case "$TRIPLOX_STORAGE" in
    memory)
        CONFIG_FILE="/etc/triplox/triplox-memory.toml"
        ;;
    local)
        CONFIG_FILE="/etc/triplox/triplox-local.toml"
        ;;
    *)
        echo "Error: unknown TRIPLOX_STORAGE value: $TRIPLOX_STORAGE"
        echo "Supported values: memory, local"
        exit 1
        ;;
esac

exec triplox "$CONFIG_FILE"
