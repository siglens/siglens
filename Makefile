GO := /usr/local/go/bin/go

lint:
	golangci-lint run --timeout=3m

ut:
	$(GO) test ./... -count 1

build:
	$(GO) mod download
	$(GO) build -o siglens cmd/siglens/main.go

run:
	$(GO) version
	$(GO) build -o siglens cmd/siglens/main.go
	./siglens --config server.yaml

all: lint build run