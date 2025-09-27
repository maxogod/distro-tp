FROM golang:1.24.6 AS builder

# Create the full path to match your imports
RUN mkdir -p /build/github.com/maxogod/distro-tp
WORKDIR /build/github.com/maxogod/distro-tp

COPY go.mod go.sum ./

# Copy worker_base to the correct location
COPY src/worker_base/ ./src/worker_base/
COPY src/common/ ./src/common/

RUN go build -o bin/worker ./src/worker_base/cmd

FROM busybox:latest
COPY --from=builder /build/github.com/maxogod/distro-tp/bin/worker /worker
ENTRYPOINT ["/worker"]