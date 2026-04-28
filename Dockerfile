# Kopia binary stage
FROM kopia/kopia:latest AS kopia

# Build stage
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

ARG TARGETARCH

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o pvc-plumber ./cmd/pvc-plumber

# Final stage - use alpine for kopia compatibility
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /

# Copy pvc-plumber binary
COPY --from=builder /build/pvc-plumber /pvc-plumber

# Copy kopia binary
COPY --from=kopia /bin/kopia /usr/local/bin/kopia

# Create non-root user matching VolSync mover UID
RUN addgroup -g 568 plumber && \
    adduser -D -u 568 -G plumber plumber

USER plumber:plumber

EXPOSE 8080

ENTRYPOINT ["/pvc-plumber"]
