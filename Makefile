lint:
	golangci-lint run --timeout=3m

test:
	go test ./... -count 1

build:
	go mod tidy
	go build -o siglens cmd/siglens/main.go

run:
	go run cmd/siglens/main.go --config server.yaml

gofmt :
	go install golang.org/x/tools/cmd/goimports@latest
	goimports -w .

all: lint test build

pr: all gofmt
