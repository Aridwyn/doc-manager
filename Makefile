.PHONY: docker-up docker-down docker-logs docker-build docker-rebuild docker-ps docker-stop test test-race test-integration test-load

docker-build:
	docker-compose build

docker-rebuild:
	docker-compose build --no-cache

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-logs-app:
	docker-compose logs -f app

docker-logs-reindexer:
	docker-compose logs -f reindexer

docker-ps:
	docker-compose ps

docker-stop:
	docker-compose stop

docker-restart:
	docker-compose restart

docker-clean:
	docker-compose down -v

# Test commands
test:
	@echo "Starting Reindexer for tests..."
	@docker-compose up -d reindexer
	@sleep 10
	@echo "Running tests..."
	@CGO_ENABLED=1 go test -v ./... || (docker-compose stop reindexer && exit 1)
	@echo "Stopping Reindexer..."
	@docker-compose stop reindexer

test-race:
	@echo "Starting Reindexer for race tests..."
	@docker-compose up -d reindexer
	@sleep 10
	@echo "Running race tests..."
	@CGO_ENABLED=1 go test -v -race ./... || (docker-compose stop reindexer && exit 1)
	@echo "Stopping Reindexer..."
	@docker-compose stop reindexer

test-short:
	go test -v -short ./...

test-integration:
	@echo "Starting Reindexer for integration tests..."
	@docker-compose up -d reindexer
	@sleep 10
	@echo "Running integration tests..."
	@CGO_ENABLED=1 go test -v -timeout=5m ./internal/repositories/... || (docker-compose down && exit 1)
	@echo "Stopping Reindexer..."
	@docker-compose stop reindexer

test-load:
	go test -v -timeout=10m -run TestLoad ./load_test.go

test-coverage:
	go test -v -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html

test-all: test-race test-integration
