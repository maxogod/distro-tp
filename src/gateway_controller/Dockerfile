FROM golang:1.24.6 AS builder

# Create the full path to match your imports
RUN mkdir -p /build/github.com/maxogod/distro-tp
WORKDIR /build/github.com/maxogod/distro-tp

COPY go.mod go.sum ./

# Copy worker_base to the correct location
COPY src/gateway_controller/ ./src/gateway_controller/
COPY src/common/ ./src/common/

RUN go build -o bin/gateway_controller ./src/gateway_controller/cmd

FROM busybox:latest
COPY --from=builder /build/github.com/maxogod/distro-tp/bin/gateway_controller /gateway_controller
ENTRYPOINT ["/gateway_controller"]