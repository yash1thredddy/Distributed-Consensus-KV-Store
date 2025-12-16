# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o /app/server \
    ./cmd/server

# Runtime stage
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user for security
RUN adduser -D -g '' appuser

# Create data directory
RUN mkdir -p /data && chown appuser:appuser /data

# Copy binary from builder
COPY --from=builder /app/server /usr/local/bin/server

# Copy docker-specific config files
COPY --from=builder /app/configs/docker /etc/distributed-kv/configs

# Set working directory
WORKDIR /app

# Use non-root user
USER appuser

# Expose ports
# 5001-5005: Raft RPC ports
# 8080: HTTP API port
EXPOSE 5001 8080

# Data volume
VOLUME ["/data"]

# Health check
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Default command
ENTRYPOINT ["/usr/local/bin/server"]
CMD ["--config", "/etc/distributed-kv/configs/node1.yaml"]
