# Builder stage
FROM golang:1.21-bookworm AS builder

# Install build dependencies including CGO requirements
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    make \
    gcc \
    g++ \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod ./
COPY go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Build the application with CGO enabled for cproto binding
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags='-w -s' \
    -o /app/server \
    ./cmd/server

# Final stage - use Debian for CGO library compatibility
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libstdc++6 \
    libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -g 1000 appuser && \
    useradd -u 1000 -g appuser -s /bin/sh -m appuser

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/server .

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the application
CMD ["./server"]
