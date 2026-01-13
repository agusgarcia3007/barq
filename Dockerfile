# Stage 1: Build environment
FROM rust:1.75-bookworm AS builder

# Install io_uring development libraries and clang for optimized builds
RUN apt-get update && apt-get install -y \
    liburing-dev \
    clang \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Native CPU optimizations for maximum SIMD utilization
ENV RUSTFLAGS="-C target-cpu=native"

RUN cargo build --release

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

# Runtime dependency for io_uring
RUN apt-get update && apt-get install -y \
    liburing2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/barq /usr/local/bin/

CMD ["barq"]
