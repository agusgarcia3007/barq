# Barq

**Barq** (from Arabic **بَرْق** - _/bɑːrk/_, meaning "lightning") is an ultra-fast PostgreSQL-compatible database engine, built from the ground up for maximum performance.

## Features

- **PostgreSQL Compatible** - Connect using any standard PostgreSQL client (`psql`, Python, Node.js drivers, etc.)
- **Apache Arrow Columnar Storage** - Optimized for SIMD operations and blazing-fast analytical scans
- **Written in Rust** - Memory-safe, zero garbage collection, maximum efficiency
- **Async at its Core** - Built on Tokio for massive concurrency with minimal overhead

## Quick Start

### With Docker

```bash
docker run -p 5432:5432 barq
```

### From Source

```bash
cargo build --release
./target/release/barq
```

### Connect

```bash
psql -h localhost -U barq -d barq
# Password: barq
```

## Configuration

| Variable        | Description | Default   |
| --------------- | ----------- | --------- |
| `BARQ_HOST`     | Listen host | `0.0.0.0` |
| `BARQ_PORT`     | Port        | `5432`    |
| `BARQ_USER`     | Username    | `barq`    |
| `BARQ_PASSWORD` | Password    | `barq`    |

## Why "Barq"?

The name comes from Arabic **بَرْق** (pronounced _barq_), meaning **"lightning"**. It represents the extreme speed we aim for: queries that execute at the speed of light.

## License

MIT
