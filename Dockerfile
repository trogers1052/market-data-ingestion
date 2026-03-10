# Build stage
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

ARG TARGETARCH

# Install timezone data for the builder
RUN apk add --no-cache tzdata ca-certificates git

WORKDIR /app

COPY go.mod go.sum ./
RUN sed -i '/replace.*trading-testkit/d' go.mod && \
    GOPRIVATE=github.com/trogers1052/* go get github.com/trogers1052/trading-testkit@v0.2.0 && \
    go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /market-data-ingestion \
    ./cmd/ingestion
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /healthcheck \
    ./cmd/healthcheck

# Run stage
FROM gcr.io/distroless/static-debian12

# Copy timezone data and CA certificates from builder
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /market-data-ingestion /market-data-ingestion
COPY --from=builder /healthcheck /healthcheck

# Copy migrations directory
COPY --from=builder /app/db/migrations /db/migrations

USER nonroot:nonroot

ENV TZ=America/New_York

HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=15s \
    CMD ["/healthcheck"]

ENTRYPOINT ["/market-data-ingestion"]
