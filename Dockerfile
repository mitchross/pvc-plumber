# Kopia binary stage
FROM kopia/kopia:latest AS kopia

# Build stage
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

ARG TARGETARCH

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary — ./cmd/operator is the v2 entrypoint that wires the
# controller-runtime manager + admission webhooks. It still hosts the legacy
# /exists HTTP server when OPERATOR_MODE=false, so this single binary covers
# both deployment shapes. Building ./cmd/pvc-plumber here was the v2.1.0
# regression that left the cluster's webhook server dead.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o pvc-plumber ./cmd/operator

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

# 8080: legacy /exists HTTP API + Prometheus /metrics + /healthz/readyz.
# 9443: TLS admission webhook server (controller-runtime), used when
#       OPERATOR_MODE=true.
EXPOSE 8080
EXPOSE 9443

ENTRYPOINT ["/pvc-plumber"]
