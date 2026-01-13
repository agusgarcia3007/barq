# --- ETAPA 1: Builder (Compilación) ---
FROM rust:1.85-bookworm AS builder

WORKDIR /usr/src/barq

# Instalamos dependencias de sistema para compilación
RUN apt-get update && apt-get install -y clang lld && rm -rf /var/lib/apt/lists/*

# Copiamos los archivos del proyecto
COPY . .

# Compilamos en modo release
RUN cargo build --release

# --- ETAPA 2: Runner (Imagen Final Ligera) ---
FROM debian:bookworm-slim

# Copiamos el binario desde la etapa anterior
COPY --from=builder /usr/src/barq/target/release/barq /usr/local/bin/barq

# Exponemos el puerto TCP
EXPOSE 8080

# Comando de arranque
CMD ["barq"]
