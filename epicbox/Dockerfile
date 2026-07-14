# syntax=docker/dockerfile:1

# rust builder stages
FROM debian:bookworm-slim AS rust-builder

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        curl \
        ca-certificates \
        clang \
        libclang-dev \
        llvm-dev \
        libncursesw6 \
        cmake \
        build-essential \
        git \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /usr/src/epicboxlib_source

# copy the binary crate and its library dependency
COPY epicboxlib_source/Cargo.toml ./Cargo.toml
COPY epicboxlib_source/Cargo.lock ./Cargo.lock
COPY epicboxlib_source/src ./src
COPY epicboxlib_source/epicboxlib ./epicboxlib
# epic-wallet submodule for local path resolution
COPY epicboxlib_source/epic-wallet ./epic-wallet

# buildKit cache mounts persist the cargo registry and build artifacts
# across builds, which speeds up rebuild. The binary must be copied out
# of the cache mount because the cache is not part of the image layer
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/usr/src/epicboxlib_source/target \
    cargo build --release && \
    cp target/release/epicboxlib /epicboxlib

# node js deps stages
FROM node:22-bookworm-slim AS node-deps

WORKDIR /usr/src/app

# copy only the manifests first so this layer is cached until
# package.json / package-lock.json change
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# runtime stages
FROM node:22-bookworm-slim

# runtime libs needed by rust bin, plus locales
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        locales \
        openssl \
        libncursesw6 \
    && rm -rf /var/lib/apt/lists/* && \
    sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen

ENV LANG=en_US.UTF-8 \
    NODE_ENV=production

# dedicated non-root service user. app dir must be owned by the
# user so the app/epicboxlib can write runtime files in the workdir
RUN useradd -u 1001 -U -m -s /bin/bash epicsvcs && \
    mkdir -p /usr/src/app && \
    chown epicsvcs:epicsvcs /usr/src/app

WORKDIR /usr/src/app

# Pre-built node_modules from the deps stage
COPY --from=node-deps --chown=epicsvcs:epicsvcs /usr/src/app/node_modules ./node_modules

COPY --chown=epicsvcs:epicsvcs package.json app_mongo.js ./

# default config shipped as config.json for app compatibility
COPY --chown=epicsvcs:epicsvcs default_config.json ./config.json

# compiled rust bin from the builder stage
COPY --from=rust-builder --chown=epicsvcs:epicsvcs --chmod=755 /epicboxlib ./epicboxlib

USER epicsvcs

# app listens on 3423 in-container (compose maps 127.0.0.1:8888/8889 -> 3423)
EXPOSE 3423

# any http response (even 400/426 from the websocket endpoint) counts as alive
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD node -e "require('http').get('http://127.0.0.1:3423', () => process.exit(0)).on('error', () => process.exit(1))"

CMD ["node", "app_mongo.js"]
