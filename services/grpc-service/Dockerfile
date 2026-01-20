FROM golang:1.21-alpine AS build

WORKDIR /app

COPY go.mod go.sum ./
COPY pb ./pb
COPY main.go .

RUN go build -o /app/grpc-service .

FROM alpine:3.20

COPY --from=build /app/grpc-service /grpc-service

EXPOSE 6000

CMD ["/grpc-service"]
