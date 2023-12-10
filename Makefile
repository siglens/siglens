

GO := $(shell which go)
ifeq ($(GO), )
	GO := /usr/local/go/bin/go
endif
export GO 


lint:
	golangci-lint run --timeout=3m

test:
	$(GO) test ./... -count 1

build:
	$(GO) mod tidy
	$(GO) build -o siglens cmd/siglens/main.go

run:
	$(GO) run cmd/siglens/main.go --config server.yaml

gofmt :
	$(GO) install golang.org/x/tools/cmd/goimports@latest
	~/go/bin/goimports -w .

all: lint test build

pr: all gofmt
