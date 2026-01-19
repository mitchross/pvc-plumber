# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o pvc-plumber ./cmd/pvc-plumber

# Final stage
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

# Copy binary from builder
COPY --from=builder /build/pvc-plumber /pvc-plumber

# Use non-root user (already set in distroless/nonroot)
USER nonroot:nonroot

EXPOSE 8080

ENTRYPOINT ["/pvc-plumber"]
