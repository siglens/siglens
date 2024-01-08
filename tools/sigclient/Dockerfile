FROM golang:1.18-alpine3.17

WORKDIR /usr/app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o sigclient main.go

CMD ["./sigclient"]