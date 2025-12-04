# ** Download dependencies **
FROM golang:1.24.6 AS deps

COPY go.mod go.sum ./
RUN go mod download

# ** Compile code **
FROM golang:1.24.6 AS builder

WORKDIR /app

ARG BUILD_TAGS=""

COPY go.sum go.mod ./
COPY --from=deps /go/pkg/mod /go/pkg/mod
COPY src/common/ ./src/common/
COPY src/controller/ ./src/controller/

RUN if [ -n "$BUILD_TAGS" ]; then \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags="$BUILD_TAGS" -ldflags="-s -w" -v -o ./app ./src/controller/cmd; \
    else \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -v -o ./app ./src/controller/cmd; \
    fi

# ** Run binary **
FROM alpine:latest AS runner

WORKDIR /app

RUN apk --no-cache add curl

COPY --from=builder /app/app ./app

ENTRYPOINT ["./app"]
