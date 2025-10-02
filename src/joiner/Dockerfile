FROM golang:1.24.6 AS builder

# Create the full path to match your imports
RUN mkdir -p /build/github.com/maxogod/distro-tp
WORKDIR /build/github.com/maxogod/distro-tp

COPY go.mod go.sum ./

# Copy worker_base to the correct location
COPY src/joiner/ ./src/joiner/
COPY src/common/ ./src/common/

RUN go build -o bin/joiner ./src/joiner/cmd

FROM busybox:latest
COPY --from=builder /build/github.com/maxogod/distro-tp/bin/joiner /joiner
ENTRYPOINT ["/joiner"]