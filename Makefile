.PHONY: all proto build test test-race test-cover lint docker-build cluster-up cluster-down integration clean

# Default target
all: proto build test

# Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	protoc --go_out=. --go_opt=module=github.com/yourusername/distributed-kv --go-grpc_out=. --go-grpc_opt=module=github.com/yourusername/distributed-kv api/proto/raft.proto
	protoc --go_out=. --go_opt=module=github.com/yourusername/distributed-kv --go-grpc_out=. --go-grpc_opt=module=github.com/yourusername/distributed-kv api/proto/kv.proto
	@echo "Protobuf code generation complete!"

# Build the server binary
build:
	@echo "Building server..."
	go build -o bin/server.exe ./cmd/server

# Run unit tests
test:
	@echo "Running tests..."
	go test ./... -v

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	go test ./... -race -v

# Run tests with coverage
test-cover:
	@echo "Running tests with coverage..."
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run ./...

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -f deployments/docker/Dockerfile -t distributed-kv:latest .

# Start Docker Compose cluster
cluster-up:
	@echo "Starting cluster..."
	docker-compose -f deployments/docker-compose.yml up -d

# Stop Docker Compose cluster
cluster-down:
	@echo "Stopping cluster..."
	docker-compose -f deployments/docker-compose.yml down

# Run integration tests
integration:
	@echo "Running integration tests..."
	go test ./test/integration/... -v

# Clean build artifacts
clean:
	@echo "Cleaning..."
	if exist bin rmdir /s /q bin
	if exist coverage.out del coverage.out
	if exist coverage.html del coverage.html
	if exist data rmdir /s /q data
