# syntax=docker/dockerfile:1

# Build the application from source
FROM golang:1.24 AS build-stage

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go .
RUN CGO_ENABLED=0 GOOS=linux go build -o main

COPY . .

#RUN NOTIFY_DEBUG=1 go test -test.v -test.run TestNotifyReveiverListenToNewDir .
RUN go test -test.v .
