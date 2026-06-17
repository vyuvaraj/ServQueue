FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o servqueue main.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/servqueue .
EXPOSE 8082 61613
ENTRYPOINT ["./servqueue"]
