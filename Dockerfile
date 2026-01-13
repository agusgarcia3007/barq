# --- ETAPA 1: Builder (Compilación) ---
FROM rust:1.85-bookworm AS builder

WORKDIR /usr/src/barq

# Instalamos dependencias de sistema para compilación
RUN apt-get update && apt-get install -y clang lld protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Copiamos los archivos del proyecto
COPY . .

# Compilamos en modo release
RUN cargo build --release

# --- ETAPA 2: Runner (Imagen Final Ligera) ---
FROM debian:bookworm-slim

# Copiamos el binario desde la etapa anterior
COPY --from=builder /usr/src/barq/target/release/barq /usr/local/bin/barq

# Variables de entorno por defecto
ENV BARQ_HOST=0.0.0.0
ENV BARQ_PORT=5432
ENV BARQ_USER=barq
ENV BARQ_PASSWORD=barq
ENV RUST_LOG=barq=info

# Exponemos el puerto PostgreSQL
EXPOSE 5432

# Comando de arranque
CMD ["barq"]
