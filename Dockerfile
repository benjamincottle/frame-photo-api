
# Force the builder stage to ALWAYS run natively on your host architecture (amd64)
# This prevents Docker from trying to run the Rust compiler inside slow ARM emulation
FROM --platform=$BUILDPLATFORM rust:1-slim-trixie AS builder

# Install the aarch64 cross-linker (only strictly needed if we are building for arm64)
RUN apt-get update && apt-get install -y \
    g++-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add aarch64-unknown-linux-gnu 

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse \
    RUSTFLAGS="-C link-arg=-s"

WORKDIR /app
COPY . .

# Bring in the target architecture variable automatically supplied by Buildx
ARG TARGETARCH

# Dynamically compile based on what Buildx requested for this specific pass
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        cargo build --release --target aarch64-unknown-linux-gnu && \
        cp target/aarch64-unknown-linux-gnu/release/photo_api /app/photo_api-final; \
    else \
        cargo build --release && \
        cp target/release/photo_api /app/photo_api-final; \
    fi

# --- Final Stage ---
# This automatically pulls the matching debian image for the target platform
FROM gcr.io/distroless/cc-debian13
WORKDIR /app

# Copy the correctly built binary from the temporary location
COPY --from=builder /app/photo_api-final /app/photo_api

EXPOSE 5000
CMD ["./photo_api"]
