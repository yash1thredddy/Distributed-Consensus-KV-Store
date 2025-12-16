.PHONY: all proto build test test-race test-cover lint docker-build cluster-up cluster-down cluster-logs cluster-restart integration clean web-install web-build web-dev run-node1 run-node2 run-node3 run-local

# Default target
all: proto build test

# Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	protoc --go_out=. --go_opt=module=github.com/yash1thredddy/Distributed-Consensus-KV-Store --go-grpc_out=. --go-grpc_opt=module=github.com/yash1thredddy/Distributed-Consensus-KV-Store api/proto/raft.proto
	protoc --go_out=. --go_opt=module=github.com/yash1thredddy/Distributed-Consensus-KV-Store --go-grpc_out=. --go-grpc_opt=module=github.com/yash1thredddy/Distributed-Consensus-KV-Store api/proto/kv.proto
	@echo "Protobuf code generation complete!"

# Build the server binary
build:
	@echo "Building server..."
	go build -o bin/server.exe ./cmd/server

# Build for Linux (for Docker)
build-linux:
	@echo "Building server for Linux..."
	GOOS=linux GOARCH=amd64 go build -o bin/server ./cmd/server

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
	docker-compose -f deployments/docker/docker-compose.yml up -d

# Stop Docker Compose cluster
cluster-down:
	@echo "Stopping cluster..."
	docker-compose -f deployments/docker/docker-compose.yml down

# View cluster logs
cluster-logs:
	@echo "Showing cluster logs..."
	docker-compose -f deployments/docker/docker-compose.yml logs -f

# Restart cluster
cluster-restart: cluster-down cluster-up

# Remove cluster volumes (WARNING: deletes all data)
cluster-clean:
	@echo "Removing cluster volumes..."
	docker-compose -f deployments/docker/docker-compose.yml down -v

# Run integration tests
integration:
	@echo "Running integration tests..."
	go test ./test/integration/... -v

# Install web dependencies
web-install:
	@echo "Installing web dependencies..."
	cd web && npm install

# Build web dashboard
web-build:
	@echo "Building web dashboard..."
	cd web && npm run build

# Run web dashboard in dev mode
web-dev:
	@echo "Starting web dashboard in dev mode..."
	cd web && npm run dev

# Run individual nodes locally (for development)
run-node1:
	@echo "Starting node1..."
	go run ./cmd/server --config=configs/node1.yaml

run-node2:
	@echo "Starting node2..."
	go run ./cmd/server --config=configs/node2.yaml

run-node3:
	@echo "Starting node3..."
	go run ./cmd/server --config=configs/node3.yaml

# Run a 3-node local cluster (requires multiple terminals)
run-local:
	@echo "To run a local cluster, open 3 terminals and run:"
	@echo "  Terminal 1: make run-node1"
	@echo "  Terminal 2: make run-node2"
	@echo "  Terminal 3: make run-node3"

# Clean build artifacts
clean:
	@echo "Cleaning..."
	if exist bin rmdir /s /q bin
	if exist coverage.out del coverage.out
	if exist coverage.html del coverage.html
	if exist data rmdir /s /q data

# Help
help:
	@echo "Available targets:"
	@echo "  all           - Build and test everything"
	@echo "  proto         - Generate protobuf code"
	@echo "  build         - Build server binary"
	@echo "  build-linux   - Build server binary for Linux"
	@echo "  test          - Run unit tests"
	@echo "  test-race     - Run tests with race detector"
	@echo "  test-cover    - Run tests with coverage report"
	@echo "  lint          - Run linter"
	@echo "  docker-build  - Build Docker image"
	@echo "  cluster-up    - Start Docker cluster"
	@echo "  cluster-down  - Stop Docker cluster"
	@echo "  cluster-logs  - View cluster logs"
	@echo "  cluster-restart - Restart cluster"
	@echo "  cluster-clean - Remove cluster and volumes"
	@echo "  integration   - Run integration tests"
	@echo "  web-install   - Install web dependencies"
	@echo "  web-build     - Build web dashboard"
	@echo "  web-dev       - Run web dashboard in dev mode"
	@echo "  run-node1/2/3 - Run individual nodes locally"
	@echo "  run-local     - Instructions for local cluster"
	@echo "  clean         - Clean build artifacts"
	@echo "  help          - Show this help"
