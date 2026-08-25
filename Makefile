APP        := mneme
VERSION    ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
LDFLAGS    := -s -w -X main.version=$(VERSION)
BIN_DIR    := bin
IMAGE      ?= mnemosyne/mneme:latest

.PHONY: all build build-file-adapter run test test-cover lint fmt vet tidy clean docker docker-push deps-up deps-down

all: build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(APP) ./cmd/mneme

build-file-adapter:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/file-adapter ./cmd/file-adapter

run:
	go run ./cmd/mneme

test:
	go test ./... -race -count=1

test-cover:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out | tail -n 1

fmt:
	gofmt -s -w .

vet:
	go vet ./...

lint: fmt vet

tidy:
	go mod tidy

clean:
	rm -rf $(BIN_DIR) coverage.out

docker:
	docker build -t $(IMAGE) .

docker-push:
	docker push $(IMAGE)

deps-up:
	docker compose -f deploy/docker-compose.yaml up -d

deps-down:
	docker compose -f deploy/docker-compose.yaml down -v
