FROM rust:1.88 AS builder

RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /app

# Copy the entire repository (workspace root)
COPY . .

WORKDIR /app/performer

RUN cargo build --release

FROM rust:1.88-slim

# Passed by couchbaselabs/sdk-docker-build-action (build-arg SDK=...). Declared
# so the build arg is consumed cleanly and recorded as a label.
ARG SDK=rust

LABEL org.opencontainers.image.title="${SDK}-fit-performer" \
      org.opencontainers.image.description="Couchbase Rust SDK FIT performer (gRPC server)"

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/fit-performer .
ENTRYPOINT ["./fit-performer"]
