# Build stage
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

ARG TARGETARCH

# Install timezone data for the builder
RUN apk add --no-cache tzdata ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /market-data-ingestion \
    ./cmd/ingestion

# Run stage
FROM gcr.io/distroless/static-debian12

# Copy timezone data and CA certificates from builder
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /market-data-ingestion /market-data-ingestion

# Copy migrations directory
COPY --from=builder /app/db/migrations /db/migrations

USER nonroot:nonroot

ENV TZ=America/New_York

ENTRYPOINT ["/market-data-ingestion"]
