FROM golang:1.21.3-alpine as builder
WORKDIR /app

RUN apk add --no-cache \
    gcc \
    musl-dev \
    ca-certificates

COPY . .
RUN GOOS=linux CGO_ENABLED=0 go build -o main -tags lambda.norpc main.go

FROM scratch
WORKDIR /app

LABEL maintainer="Said Rodrigues"
LABEL email="coderflemis@gmail.com"

COPY --from=builder /app/main ./
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

CMD ["./main"]
