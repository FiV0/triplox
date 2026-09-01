#!/bin/bash
set -e

# Explicit arguments take precedence over environment-based configuration.
if [ $# -gt 0 ]; then
    exec triplox "$@"
fi

if [ -n "${TRIPLOX_CONFIG_FILE:-}" ]; then
    CONFIG_FILE="$TRIPLOX_CONFIG_FILE"
else
    TRIPLOX_STORAGE="${TRIPLOX_STORAGE:-memory}"

    case "$TRIPLOX_STORAGE" in
        dev)
            CONFIG_FILE="/etc/triplox/triplox-dev.toml"
            ;;
        memory)
            CONFIG_FILE="/etc/triplox/config.toml"
            ;;
        local)
            CONFIG_FILE="/etc/triplox/triplox-local.toml"
            ;;
        remote)
            CONFIG_FILE="/etc/triplox/triplox-remote.toml"
            ;;
        kafka)
            CONFIG_FILE="/etc/triplox/triplox-kafka.toml"
            ;;
        *)
            echo "Error: unknown TRIPLOX_STORAGE value: $TRIPLOX_STORAGE"
            echo "Supported values: dev, memory, local, remote, kafka"
            exit 1
            ;;
    esac
fi

exec triplox "$CONFIG_FILE"
