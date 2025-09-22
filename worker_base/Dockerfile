FROM golang:1.23.0 AS builder

# Create the full path to match your imports
RUN mkdir -p /build/coffee-analisis
WORKDIR /build/coffee-analisis

COPY go.mod go.sum ./

# Copy worker_base to the correct location
COPY worker_base/ ./worker_base/

COPY src/common/ ./common/

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/worker ./worker_base/cmd

FROM busybox:latest
COPY --from=builder /build/coffee-analisis/bin/worker /worker
ENTRYPOINT ["/worker"]