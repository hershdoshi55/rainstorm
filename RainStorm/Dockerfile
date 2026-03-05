# Stage 1: build the daemon and ctl binaries
FROM golang:1.24-alpine AS builder

WORKDIR /build
COPY src/ .
RUN CGO_ENABLED=0 go build -o /app/rainstormd ./main && \
    CGO_ENABLED=0 go build -o /app/rainstormctl ./ctl

# Stage 2: minimal runtime image
FROM alpine:latest

# Copy compiled Go binaries
COPY --from=builder /app/rainstormd  /app/rainstormd
COPY --from=builder /app/rainstormctl /app/rainstormctl

# Copy pre-compiled Linux x86-64 operator binaries (statically linked ELF)
COPY src/rainstorm/binaries/ /home/mp4/binaries/
RUN chmod +x /home/mp4/binaries/*

# Copy entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
