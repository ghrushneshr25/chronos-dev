.PHONY: setup start stop test lint build proto docker-up docker-down logs bench tidy

export PATH := $(shell go env GOPATH)/bin:/opt/homebrew/bin:$(PATH)

setup: tidy proto
	@echo "Chronos setup complete. Run: make start"

tidy:
	go mod tidy

proto:
	protoc --go_out=. --go_opt=module=github.com/ghrushneshr25/chronos-dev \
		--go-grpc_out=. --go-grpc_opt=module=github.com/ghrushneshr25/chronos-dev \
		proto/chronos/v1/worker.proto

build:
	go build -o bin/api ./cmd/api
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/worker ./cmd/worker
	go build -o bin/outbox-publisher ./cmd/outbox-publisher
	go build -o bin/analytics-consumer ./cmd/analytics-consumer
	go build -o bin/event-replay ./cmd/event-replay

test:
	go test ./... -count=1

test-unit:
	go test ./internal/... -count=1

test-e2e:
	go test ./test/e2e -count=1 -timeout 60s -v

test-docker-e2e:
	CHRONOS_E2E_DOCKER=1 go test ./test/e2e -run Docker -count=1 -timeout 90s -v

lint:
	go vet ./...

start: docker-up

stop: docker-down

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

docker-full:
	docker compose --profile scale --profile analytics up -d --build

docker-analytics:
	docker compose --profile analytics up -d --build

logs:
	docker compose logs -f api scheduler worker outbox-publisher

logs-analytics:
	docker compose logs -f analytics-consumer clickhouse

# Submit a sample builtin job (stack must be up)
smoke:
	curl -sS -X POST http://localhost:8080/v1/jobs \
		-H 'Content-Type: application/json' \
		-H 'X-API-Key: dev-api-key-change-me' \
		-d '{"name":"smoke-echo","type":"builtin","priority":8,"payload":{"handler":"echo","args":{"message":"hello chronos"}}}' | jq .

bench:
	@echo "Benchmark harness lands in Milestone 10"
