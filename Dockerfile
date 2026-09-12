FROM rust:1-slim-trixie AS builder

ENV RUSTFLAGS="-C link-arg=-s"

WORKDIR /app
COPY . .

# --locked: build exactly the dependency versions recorded in Cargo.lock.
RUN cargo build --release --locked

# --- Final Stage ---
# The :nonroot variant runs as an unprivileged user (uid 65532).
FROM gcr.io/distroless/cc-debian13:nonroot
WORKDIR /app

COPY --from=builder /app/target/release/photo_api /app/photo_api

USER nonroot
EXPOSE 5000
# Exec form: the distroless image has no shell. The binary probes its own
# listener with an OPTIONS request (see `--healthcheck` in main.rs).
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD ["/app/photo_api", "--healthcheck"]
CMD ["./photo_api"]
