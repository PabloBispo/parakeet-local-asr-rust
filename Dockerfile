# syntax=docker/dockerfile:1

# ---- builder ----
FROM rust:1-bookworm AS builder
WORKDIR /app

# Cache deps first.
COPY Cargo.toml ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release || true
RUN rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

# Collect the binary + any ONNX Runtime shared lib ort may have produced
# (download-binaries can link statically — in which case no .so is found, fine).
RUN mkdir /out \
    && cp target/release/parakeet-asr-rust /out/ \
    && (find / -name 'libonnxruntime*.so*' -exec cp {} /out/ \; 2>/dev/null || true)

# ---- runtime ----
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/parakeet-asr-rust /usr/local/bin/parakeet-asr-rust
# Place any ORT shared libs on the loader path (no-op when statically linked).
COPY --from=builder /out/ /tmp/ortlibs/
RUN find /tmp/ortlibs -name '*.so*' -exec cp {} /usr/local/lib/ \; 2>/dev/null; \
    ldconfig 2>/dev/null || true; \
    rm -rf /tmp/ortlibs

ENV MODELS_DIR=/models \
    PORT=8090 \
    ASR_MODEL=parakeet-tdt-0.6b-v3 \
    ASR_DEVICE=cpu \
    RUST_LOG=parakeet_asr_rust=info

VOLUME ["/models"]
EXPOSE 8090
ENTRYPOINT ["parakeet-asr-rust"]
