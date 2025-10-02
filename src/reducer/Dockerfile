FROM golang:1.24.6 AS builder

# Create the full path to match your imports
RUN mkdir -p /build/github.com/maxogod/distro-tp
WORKDIR /build/github.com/maxogod/distro-tp

COPY go.mod go.sum ./

# Copy worker_base to the correct location
COPY src/reduce_count/ ./src/reduce_count/
COPY src/common/ ./src/common/

RUN go build -o bin/reduce_count ./src/reduce_count/cmd

FROM busybox:latest
COPY --from=builder /build/github.com/maxogod/distro-tp/bin/reduce_count /reduce_count
ENTRYPOINT ["/reduce_count"]