# syntax=docker/dockerfile:1

FROM golang:1.20.1-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /bird-api-perms

EXPOSE 5672

CMD [ "/bird-api-perms" ]
