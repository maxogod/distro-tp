# ** Download dependencies **
FROM golang:1.24.6 AS deps

COPY go.mod go.sum ./
RUN go mod download

# ** Compile code **
FROM golang:1.24.6 AS builder

WORKDIR /app

COPY go.sum go.mod ./
COPY --from=deps /go/pkg/mod /go/pkg/mod
COPY src/common/ ./src/common/
COPY src/gateway/ ./src/gateway/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -v -o ./app ./src/gateway/cmd

# ** Run binary **
FROM alpine:latest AS runner

WORKDIR /app

RUN apk --no-cache add curl

COPY --from=builder /app/app ./app

ENTRYPOINT ["./app"]
