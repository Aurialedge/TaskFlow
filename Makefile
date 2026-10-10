.PHONY: all build test test-race bench tidy run-api run-worker docker-up docker-down

all: build test

tidy:
	go mod tidy

build:
	go build -v -o bin/api ./cmd/api
	go build -v -o bin/worker ./cmd/worker

test:
	go test -v ./...

test-race:
	go test -v -race ./...

bench:
	go test -bench=Benchmark -benchmem -run="^$$" ./tests/load

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

docker-up:
	docker compose up -d

docker-down:
	docker compose down -v
