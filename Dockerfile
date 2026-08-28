FROM rust:1-slim-trixie AS builder

ENV RUSTFLAGS="-C link-arg=-s"

WORKDIR /app
COPY . .

RUN cargo build --release

# --- Final Stage ---
FROM gcr.io/distroless/cc-debian13
WORKDIR /app

COPY --from=builder /app/target/release/photo_api /app/photo_api

EXPOSE 5000
CMD ["./photo_api"]
