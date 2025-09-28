# ** Compile code **
FROM golang:1.24.6 AS builder

WORKDIR /app

COPY go.mod go.sum ./
COPY src/gateway/ ./src/gateway/
COPY src/gateway/config.yaml ./src/gateway/config.yaml
COPY src/common/ ./src/common/

RUN go build -v -o /app/app ./src/gateway/cmd

# ** Run binary **
FROM busybox:latest

WORKDIR /app

COPY --from=builder /app/app ./app
COPY --from=builder /app/src/gateway/config.yaml ./config.yaml

ENTRYPOINT ["./app"]
