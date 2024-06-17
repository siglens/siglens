FROM golang:1.21-alpine3.18

WORKDIR /usr/app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /usr/app/tools/sigclient
RUN go build -o sigclient main.go

CMD ["./sigclient"]