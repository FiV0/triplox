# syntax=docker/dockerfile:1

# --- Stage 1: Chef base ---
FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

# --- Stage 2: Planner ---
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# --- Stage 3: Builder ---
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Copy local workspace path dependencies needed by cargo-chef.
COPY edn/ edn/
COPY triplox-client/ triplox-client/
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

# --- Stage 4: Runtime ---
FROM debian:bookworm-slim AS runtime

ARG GIT_SHA="unknown"

LABEL org.opencontainers.image.source="https://github.com/FiV0/triplox"
LABEL org.opencontainers.image.description="Triplox database server"
LABEL org.opencontainers.image.revision="${GIT_SHA}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 20000 triplox && \
    useradd -u 20000 -g triplox -m triplox

COPY --from=builder /app/target/release/triplox /usr/local/bin/triplox
COPY docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY docker/config/ /etc/triplox/

RUN chmod +x /usr/local/bin/docker-entrypoint.sh && \
    mkdir -p /var/lib/triplox && \
    chown -R triplox:triplox /var/lib/triplox

USER triplox

VOLUME /var/lib/triplox
EXPOSE 5490

ENTRYPOINT ["docker-entrypoint.sh"]
