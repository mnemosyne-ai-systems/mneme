# syntax=docker/dockerfile:1

# ---------- build ----------
FROM golang:1.27-alpine AS builder

ARG VERSION=dev
WORKDIR /src

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/mneme ./cmd/mneme \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/file-adapter ./cmd/file-adapter

# ---------- runtime ----------
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S mneme && adduser -S -G mneme mneme

WORKDIR /app

COPY --from=builder /out/mneme /app/mneme
COPY --from=builder /out/file-adapter /app/file-adapter
COPY configs/config.yaml /app/configs/config.yaml

RUN mkdir -p /app/data && chown -R mneme:mneme /app

USER mneme

ENV MNEME_SERVER_PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["/app/mneme"]
CMD ["-config", "/app/configs/config.yaml"]
