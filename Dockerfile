# syntax=docker/dockerfile:1.6
# Multi-stage build for logs-reaper. Final image bundles:
#   - Python CLI (logs-reaper subcommands)
#   - Rust pyo3 extension (logs_reaper_core wheel, installed via maturin)
#   - docker CLI client (for `logs-reaper collect` to call `docker logs -f`)
#
# Architecture: the Rust crate is built as a pyo3 cdylib. Python imports
# `logs_reaper_core` directly — there is no standalone Rust binary or
# subprocess fallback in this image. The streaming scan runs in-process and
# writes Arrow IPC files that Python re-opens zero-copy via memory mapping.
#
# Build:
#   docker build -t logs-reaper:dev .
#
# Run:
#   docker run --rm \
#     -v /var/run/docker.sock:/var/run/docker.sock:ro \
#     -v "$(pwd)/out:/work/out" \
#     -v "$(pwd)/baselines:/work/baselines" \
#     logs-reaper:dev collect --services my-service --duration 60

# --- Stage 1: build the pyo3 wheel ------------------------------------------
# Edition 2021 + std::thread::scope (1.63+) + pyo3 0.26+ (Python 3.14)
# + crossbeam-channel 0.5 require stable Rust 1.83+.
FROM python:3.14-slim-bookworm AS rust-builder
ARG DEBIAN_FRONTEND=noninteractive
ARG RUST_VERSION=1.85.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        pkg-config \
        build-essential \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# rustup picks up RUST_VERSION; -y skips prompts; profile=minimal trims doc/src.
ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain ${RUST_VERSION} --profile minimal

# maturin >=1.7 understands pyo3 0.25's abi3 / extension-module flag set.
RUN pip install --no-cache-dir 'maturin>=1.7,<2'

WORKDIR /src
COPY rust/logs_reaper_core /src/rust/logs_reaper_core
WORKDIR /src/rust/logs_reaper_core
# Build the wheel with the `python` feature so the #[pymodule] block compiles
# in. The wheel lands under /wheels/.
RUN maturin build --release --features python --out /wheels

# --- Stage 2: runtime --------------------------------------------------------
FROM python:3.14-slim-bookworm AS runtime
ARG DEBIAN_FRONTEND=noninteractive

# git (for baseline auto-commit), curl (for fetching docker CLI), ca-certificates,
# tini for signal handling, libnotify-bin for desktop notifications via the
# host's D-Bus session bus (mounted by scripts/start_logsreaper.sh).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        curl \
        ca-certificates \
        tini \
        libnotify-bin \
    && rm -rf /var/lib/apt/lists/*

# docker CLI as a static binary (no daemon, only client). Used by
# `logs-reaper collect` to talk to the host's docker socket mounted at
# /var/run/docker.sock.
ARG DOCKER_CLI_VERSION=27.3.1
RUN curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" \
        -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz -C /tmp \
    && mv /tmp/docker/docker /usr/local/bin/docker \
    && chmod +x /usr/local/bin/docker \
    && rm -rf /tmp/docker /tmp/docker.tgz

WORKDIR /app
COPY pyproject.toml README.md /app/
COPY logs_reaper /app/logs_reaper

# Install the Rust pyo3 wheel built in stage 1, plus the runtime Python deps.
# pyarrow is the heaviest dep (~70 MB) but mandatory — the scan boundary uses
# Arrow IPC + memory-mapped tables.
COPY --from=rust-builder /wheels/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/logs_reaper_core-*.whl \
    && pip install --no-cache-dir \
        pyarrow PyYAML pydantic \
        streamlit plotly pandas \
        httpx orjson \
    && pip install --no-cache-dir -e /app \
    && rm -rf /tmp/wheels

# Standard mountpoints; both writable by the workspace volumes.
VOLUME ["/work/out", "/work/baselines"]
ENV LOGS_REAPER_OUT=/work/out
ENV LOGS_REAPER_BASELINES=/work/baselines
ENV LOGS_REAPER_REGISTRY=/work/out/runs

ENTRYPOINT ["/usr/bin/tini", "--", "logs-reaper"]
CMD ["--help"]
