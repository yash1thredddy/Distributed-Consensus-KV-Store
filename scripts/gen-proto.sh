#!/bin/bash

set -e

# Script to generate Go code from protocol buffer definitions

PROTO_DIR="api/proto"
OUT_DIR="api/proto"

echo "Generating protobuf code..."

# Generate raft.proto
protoc --go_out=$OUT_DIR --go_opt=paths=source_relative \
    --go-grpc_out=$OUT_DIR --go-grpc_opt=paths=source_relative \
    $PROTO_DIR/raft.proto

# Generate kv.proto
protoc --go_out=$OUT_DIR --go_opt=paths=source_relative \
    --go-grpc_out=$OUT_DIR --go-grpc_opt=paths=source_relative \
    $PROTO_DIR/kv.proto

echo "Protobuf code generation complete!"
