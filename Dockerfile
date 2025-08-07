# build the image: docker build . -t rust_cross_compile/aarch64
# run the container: docker run --rm -v $(pwd):/app rust_cross_compile/aarch64
FROM rust:1.79.0-bookworm
RUN apt update && apt upgrade -y 
RUN apt install -y g++-aarch64-linux-gnu
RUN groupadd -g 1000 build && useradd -u 1000 -g build -s /bin/bash -m build
USER 1000:1000
WORKDIR /app 
RUN rustup target add aarch64-unknown-linux-gnu 
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse \
    RUSTFLAGS="-C link-arg=-s" 
CMD ["cargo", "build", "--release", "--target", "aarch64-unknown-linux-gnu"]
