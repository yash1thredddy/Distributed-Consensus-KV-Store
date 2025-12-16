package server

import (
	"time"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/raft"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/storage"
)

// --------------------------------------------------------------------------
// Builder Pattern for KVServerConfig
// Provides fluent API for configuring KVServer
// --------------------------------------------------------------------------

// KVServerConfigBuilder provides a fluent interface for building KVServerConfig.
type KVServerConfigBuilder struct {
	config *KVServerConfig
}

// NewKVServerConfigBuilder creates a new configuration builder.
func NewKVServerConfigBuilder() *KVServerConfigBuilder {
	return &KVServerConfigBuilder{
		config: &KVServerConfig{
			OperationTimeout: DefaultOperationTimeout,
		},
	}
}

// WithRaft sets the Raft node.
func (b *KVServerConfigBuilder) WithRaft(r *raft.RaftNode) *KVServerConfigBuilder {
	b.config.Raft = r
	return b
}

// WithStorage sets the storage implementation.
func (b *KVServerConfigBuilder) WithStorage(s storage.KVStorage) *KVServerConfigBuilder {
	b.config.Storage = s
	return b
}

// WithOperationTimeout sets the operation timeout.
func (b *KVServerConfigBuilder) WithOperationTimeout(timeout time.Duration) *KVServerConfigBuilder {
	b.config.OperationTimeout = timeout
	return b
}

// Build creates the KVServerConfig.
// Returns a copy of the configuration to prevent shared state issues.
// The builder can be reused after calling Build.
func (b *KVServerConfigBuilder) Build() *KVServerConfig {
	// Return a copy to prevent shared state between builder and returned config
	cfg := *b.config
	return &cfg
}

// --------------------------------------------------------------------------
// Functional Options Pattern for KVServer
// --------------------------------------------------------------------------

// KVServerOption is a functional option for configuring KVServer.
type KVServerOption func(*KVServerConfig)

// WithRaftOption returns an option that sets the Raft node.
func WithRaftOption(r *raft.RaftNode) KVServerOption {
	return func(cfg *KVServerConfig) {
		cfg.Raft = r
	}
}

// WithKVStorageOption returns an option that sets the storage.
func WithKVStorageOption(s storage.KVStorage) KVServerOption {
	return func(cfg *KVServerConfig) {
		cfg.Storage = s
	}
}

// WithOperationTimeoutOption returns an option that sets the operation timeout.
func WithOperationTimeoutOption(timeout time.Duration) KVServerOption {
	return func(cfg *KVServerConfig) {
		cfg.OperationTimeout = timeout
	}
}

// NewKVServerConfig creates a new KVServerConfig with options.
func NewKVServerConfig(opts ...KVServerOption) *KVServerConfig {
	cfg := &KVServerConfig{
		OperationTimeout: DefaultOperationTimeout,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}
