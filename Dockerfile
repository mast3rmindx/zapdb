FROM rust:1.72-alpine as builder

RUN apk add --no-cache musl-dev gcc

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --target x86_64-unknown-linux-musl

FROM alpine:latest

RUN apk add --no-cache libgcc

# RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/zapdb /usr/local/bin/zapdb

# USER zapdb

CMD ["zapdb"]
