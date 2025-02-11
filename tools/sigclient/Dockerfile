FROM golang:1.21-alpine3.18

WORKDIR /usr/app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /usr/app
RUN go build -o sigclient main.go

ENTRYPOINT ["./sigclient"]