# Triplox Docker

Docker support for running Triplox with in-memory or local persisted storage.

## Building

```bash
./docker/scripts/build-image.sh
```

This builds and tags the image as `triplox:latest` and `triplox:<short-sha>`.

## Running

### In-memory mode (default)

```bash
docker run -p 5490:5490 triplox:latest
```

### Local persisted mode

```bash
docker run -p 5490:5490 \
  -e TRIPLOX_STORAGE=local \
  -v triplox-data:/var/lib/triplox \
  triplox:latest
```

### Custom config

Mount your own config file and pass its path as an argument:

```bash
docker run -p 5490:5490 \
  -v ./my-config.toml:/etc/triplox/custom.toml \
  triplox:latest /etc/triplox/custom.toml
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TRIPLOX_STORAGE` | `memory` | Storage mode: `memory` or `local` |

## Ports

| Port | Protocol | Description |
|---|---|---|
| 5490 | TCP | Triplox wire protocol |

## Volumes

| Path | Description |
|---|---|
| `/var/lib/triplox` | Persistent data directory (used in `local` mode) |
