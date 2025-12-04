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
COPY src/filter/ ./src/filter/

RUN if [ -n "$BUILD_TAGS" ]; then \
    go build -tags="$BUILD_TAGS" -v -o ./app ./src/filter/cmd; \
    else \
    go build -v -o ./app ./src/filter/cmd; \
    fi

# ** Run binary **
FROM busybox:latest AS runner

WORKDIR /app

COPY --from=builder /app/app ./app

ENTRYPOINT ["./app"]
