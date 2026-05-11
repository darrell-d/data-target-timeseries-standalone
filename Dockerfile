# syntax=docker/dockerfile:1.5

# Stage 1: Build
FROM --platform=$BUILDPLATFORM golang:1.24-bullseye AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o data-target-timeseries ./cmd/timeseries

# Stage 2: Runtime
# Dual-mode image: runs as ECS task or Lambda function.
# AWS_LAMBDA_RUNTIME_API is set by the Lambda service — the Go binary
# detects it and branches to Lambda RIC mode automatically.
FROM --platform=linux/amd64 debian:bullseye-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/data-target-timeseries /usr/local/bin/data-target-timeseries

ENTRYPOINT ["/usr/local/bin/data-target-timeseries"]
