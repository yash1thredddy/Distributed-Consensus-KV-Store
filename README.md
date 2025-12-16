# Distributed Consensus KV Store

A Raft-based distributed key-value store implementation in Go.

## Project Overview

This is a production-grade distributed key-value store that uses the Raft consensus algorithm to provide:
- Strong consistency (linearizability)
- Fault tolerance (survives 2 node failures out of 5)
- Automatic leader election
- Log replication and persistence

## Tech Stack

- **Language**: Go 1.21+
- **Consensus**: Raft Protocol
- **RPC**: gRPC + Protocol Buffers
- **Storage**: RocksDB
- **Metrics**: Prometheus
- **Dashboard**: React + Vite + Tailwind

## Project Structure

```
distributed-kv/
├── cmd/server/          # Application entry point
├── internal/
│   ├── raft/           # Raft consensus implementation
│   ├── storage/        # Persistence layer
│   ├── transport/      # gRPC communication
│   ├── server/         # KV API server
│   └── metrics/        # Prometheus metrics
├── api/proto/          # Protocol buffer definitions
├── pkg/
│   ├── config/         # Configuration management
│   └── testutil/       # Test utilities
├── web/                # React dashboard
└── deployments/        # Docker and deployment configs
```

## Development

### Prerequisites

- Go 1.21 or higher
- Protocol Buffers compiler (protoc)
- RocksDB
- Docker and Docker Compose (for cluster testing)

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

### Running a Single Node

```bash
go run ./cmd/server --config=configs/node1.yaml
```

### Running a Cluster

```bash
# Start 5-node cluster
make cluster-up

# Stop cluster
make cluster-down
```

## Implementation Phases

1. **Phase 1**: Foundation (project setup, protos, types) ✓
2. **Phase 2**: Storage layer (In Progress)
3. **Phase 3**: Transport layer
4. **Phase 4**: Raft consensus core
5. **Phase 5**: KV server API
6. **Phase 6**: Observability and dashboard
7. **Phase 7**: Deployment and integration

## Documentation

- [Implementation Specification](docs/Distributed_KV_Store_Implementation_Spec.md)
- [Raft Deep Dive](.claude/docs/PHASE4_RAFT.md)
- [Testing Guide](.claude/docs/TESTING.md)
- [Common Pitfalls](.claude/docs/PITFALLS.md)

## References

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://raft.github.io/)
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)

## License

MIT
