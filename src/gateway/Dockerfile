# ** Download dependencies **
FROM golang:1.24.6 AS deps

COPY go.mod go.sum ./
RUN go mod download

# ** Compile code **
FROM golang:1.24.6 AS builder

COPY --from=deps /go/pkg/mod /go/pkg/mod
COPY src/common/ ./src/common/
COPY src/gateway/ ./src/gateway/

RUN go build -v -o /app/app ./src/gateway/cmd

# ** Run binary **
FROM busybox:latest AS runner

WORKDIR /app

COPY --from=builder /app/app ./app
COPY --from=builder /app/src/gateway/config.yaml ./config.yaml

ENTRYPOINT ["./app"]
