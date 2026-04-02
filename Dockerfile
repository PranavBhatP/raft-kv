FROM golang:1.24.0-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o raft-kv ./cmd/raft-kv

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/raft-kv .
EXPOSE 50051
CMD ["./raft-kv"]