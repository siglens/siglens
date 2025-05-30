# Copyright (c) 2021-2024 SigScalr, Inc.
#
# This file is part of SigLens Observability Solution
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

GO := $(shell which go)
ifeq ($(GO), )
	GO := /usr/local/go/bin/go
endif
export GO 


lint:
	golangci-lint run --timeout=3m --fix --max-issues-per-linter=0 --max-same-issues=0

test:
	$(GO) test ./... -count 1

test_all:
	$(GO) test ./... -count 1 --tags=e2e_all

build: pkg/ast/spl/spl.go
	$(GO) mod tidy
	$(GO) build -o siglens cmd/siglens/main.go

run:
	$(GO) run cmd/siglens/main.go --config server.yaml

gofmt :
	$(GO) install golang.org/x/tools/cmd/goimports@latest
	~/go/bin/goimports -w .

%.go: %.peg
	$(GO) install github.com/mna/pigeon@v1.1.0
	pigeon -o $@ $<

all: lint test_all build

pr: all gofmt
