# ** Compile code **
FROM golang:1.24.6 AS builder

WORKDIR /usr/src/app

COPY go.mod go.sum ./
COPY src/gateway/ ./src/gateway/
COPY src/gateway/config.yaml ./src/gateway/config.yaml
COPY src/common/ ./src/common/

RUN go build -v -o /usr/local/bin/app ./src/gateway/cmd

# ** Run binary **
FROM busybox:latest

COPY --from=builder /usr/local/bin/app /usr/local/bin/app
COPY --from=builder /usr/src/app/src/gateway/config.yaml ./config.yaml

ENTRYPOINT ["/usr/local/bin/app"]
