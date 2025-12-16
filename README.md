# Distributed Consensus KV Store

A Raft-based distributed key-value store implementation in Go, providing strong consistency and fault tolerance for distributed systems.

## What Is This?

This is a distributed key-value store that uses the **Raft consensus algorithm** to ensure data consistency across multiple nodes. It's designed to be:

- **Strongly consistent** - All reads return the most recent write (linearizability)
- **Fault tolerant** - Continues operating even when nodes fail (survives N/2-1 failures)
- **Self-healing** - Automatically elects new leaders and recovers from failures
- **Persistent** - Data survives node restarts through durable storage

## Why Use This?

Traditional single-node databases are single points of failure. This distributed KV store solves that by:

1. **Replicating data** across multiple nodes so no single failure loses data
2. **Using consensus** to ensure all nodes agree on the order of operations
3. **Providing automatic failover** when the leader node goes down
4. **Guaranteeing consistency** even during network partitions (CP in CAP theorem)

### Use Cases

- Configuration management for distributed systems
- Service discovery and coordination
- Distributed locking and leader election
- Metadata storage for larger distributed systems

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                                │
└─────────────────────────┬───────────────────────────────────┘
                          │ HTTP/gRPC
┌─────────────────────────▼───────────────────────────────────┐
│                    KV Server Layer                           │
│              (Put, Get, Delete operations)                   │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Raft Consensus                            │
│         (Leader election, Log replication)                   │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   Storage Layer                              │
│              (BadgerDB / In-memory)                          │
└─────────────────────────────────────────────────────────────┘
```

### Raft Consensus

The system implements the Raft protocol which ensures:

- **Leader Election**: One node is elected leader; others are followers
- **Log Replication**: Leader replicates all writes to followers
- **Safety**: Only logs replicated to a majority are committed
- **Persistence**: Term, vote, and log entries survive restarts

### Request Flow

1. Client sends write request to any node
2. If not leader, request is redirected to the leader
3. Leader appends entry to its log
4. Leader replicates entry to followers via AppendEntries RPC
5. Once majority acknowledges, entry is committed
6. Leader applies to state machine and responds to client

## Project Structure

```
distributed-kv/
├── cmd/server/              # Application entry point
├── configs/                 # Node configuration files
├── internal/
│   ├── raft/               # Raft consensus implementation
│   │   ├── raft.go         # Core RaftNode logic
│   │   ├── log.go          # Log management
│   │   ├── election.go     # Leader election
│   │   ├── replication.go  # Log replication
│   │   └── rpc_handlers.go # RPC request handlers
│   ├── storage/            # Persistence layer
│   │   ├── interface.go    # Storage interface
│   │   ├── memory.go       # In-memory implementation
│   │   └── badger.go       # BadgerDB implementation
│   ├── transport/          # gRPC communication
│   └── server/             # KV API server
├── api/proto/              # Protocol buffer definitions
├── pkg/
│   ├── config/             # Configuration management
│   └── testutil/           # Test utilities
└── deployments/            # Docker and deployment configs
```

## Tech Stack

| Component | Technology | Why |
|-----------|------------|-----|
| Language | Go 1.21+ | Excellent concurrency, static typing, fast compilation |
| Consensus | Raft | Understandable, proven, widely adopted |
| RPC | gRPC + Protobuf | Efficient binary protocol, strong typing |
| Storage | BadgerDB | Pure Go, LSM-tree based, no CGO required |
| Config | Viper | Flexible configuration from files and env vars |
| Logging | Zap | High-performance structured logging |
| Metrics | Prometheus | Industry standard for observability |

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Protocol Buffers compiler (protoc)
- Docker and Docker Compose (for cluster deployment)

### Build

```bash
# Generate protobuf code
make proto

# Build the server
make build

# Run tests
make test

# Run tests with race detector
make test-race
```

### Run a Single Node

```bash
go run ./cmd/server --config=configs/node1.yaml
```

### Run a 5-Node Cluster

```bash
# Start cluster
make cluster-up

# View logs
docker-compose logs -f

# Stop cluster
make cluster-down
```

### API Usage

```bash
# Put a key
curl -X PUT http://localhost:8080/kv/mykey -d '{"value": "myvalue"}'

# Get a key
curl http://localhost:8080/kv/mykey

# Delete a key
curl -X DELETE http://localhost:8080/kv/mykey
```

## Configuration

Configuration can be provided via YAML file or environment variables:

```yaml
node_id: "node1"
data_dir: "./data/node1"
raft_addr: ":5001"
http_addr: ":8080"
peers:
  - "node2:5002"
  - "node3:5003"
election_timeout_min: 150ms
election_timeout_max: 300ms
heartbeat_interval: 50ms
snapshot_threshold: 10000
```

Environment variables use the `RAFT_` prefix:
- `RAFT_NODE_ID` - Node identifier
- `RAFT_DATA_DIR` - Data storage directory
- `RAFT_ADDR` - Raft RPC address
- `RAFT_HTTP_ADDR` - HTTP API address
- `RAFT_PEERS` - Comma-separated peer addresses
- `RAFT_ELECTION_TIMEOUT_MIN` - Minimum election timeout
- `RAFT_ELECTION_TIMEOUT_MAX` - Maximum election timeout
- `RAFT_HEARTBEAT_INTERVAL` - Heartbeat interval
- `RAFT_SNAPSHOT_THRESHOLD` - Log entries before snapshot

## Testing

```bash
# Run all tests
go test ./...

# Run with race detector (recommended)
go test ./... -race

# Run specific package
go test ./internal/raft/... -v

# Run with coverage
go test ./... -coverprofile=cover.out
go tool cover -html=cover.out
```

## References

- [Raft Paper](https://raft.github.io/raft.pdf) - The original Raft consensus paper
- [Raft Visualization](https://raft.github.io/) - Interactive visualization of Raft
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/) - Practical implementation guide

## License

MIT
