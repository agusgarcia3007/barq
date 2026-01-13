# --- ETAPA 1: Builder (Compilación) ---
FROM rust:1.85-bookworm AS builder

WORKDIR /usr/src/barq

# Instalamos dependencias de sistema necesarias para Glommio/io_uring
RUN apt-get update && apt-get install -y liburing-dev clang lld

# Copiamos los archivos del proyecto
COPY . .

# Compilamos en modo release
# RUSTFLAGS permite optimizar para la CPU destino si fuera necesario
RUN cargo build --release

# --- ETAPA 2: Runner (Imagen Final Ligera) ---
FROM debian:bookworm-slim

# Instalamos solo la librería runtime de io_uring
RUN apt-get update && apt-get install -y liburing2 && rm -rf /var/lib/apt/lists/*

# Copiamos el binario desde la etapa anterior
COPY --from=builder /usr/src/barq/target/release/barq /usr/local/bin/barq

# Exponemos el puerto (si tu app escucha en alguno, ej 8080)
# EXPOSE 8080

# Comando de arranque
CMD ["barq"]
