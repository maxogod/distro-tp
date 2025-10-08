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
COPY src/group_by/ ./src/group_by/

RUN go build -v -o ./app ./src/group_by/cmd

# ** Run binary **
FROM busybox:latest AS runner

WORKDIR /app

COPY --from=builder /app/app ./app
COPY --from=builder /app/src/group_by/config.yaml ./config.yaml

ENTRYPOINT ["./app"]
