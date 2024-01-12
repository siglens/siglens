FROM golang:1.21-alpine3.18

WORKDIR /usr/app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o sigclient main.go

CMD ["./sigclient"]