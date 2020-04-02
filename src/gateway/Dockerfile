# Build stage
FROM golang:1.13-alpine3.10 AS build
WORKDIR /src
COPY gateway.go .
ENV CGO_ENABLED=0
RUN go build -o gateway gateway.go

# Deployment stage
FROM alpine:3.10
WORKDIR /app
COPY --from=build /src/gateway .
EXPOSE 8080
ENTRYPOINT [ "./gateway" ]
