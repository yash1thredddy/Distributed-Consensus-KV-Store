package raft

import (
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/storage"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/transport"
)

// --------------------------------------------------------------------------
// Builder Pattern for RaftConfig
// Provides fluent API for configuring RaftNode
// --------------------------------------------------------------------------

// RaftConfigBuilder provides a fluent interface for building RaftConfig.
type RaftConfigBuilder struct {
	config *RaftConfig
}

// NewRaftConfigBuilder creates a new configuration builder with required fields.
func NewRaftConfigBuilder(id string) *RaftConfigBuilder {
	return &RaftConfigBuilder{
		config: &RaftConfig{
			ID:                 id,
			ElectionTimeoutMin: DefaultElectionTimeoutMin,
			ElectionTimeoutMax: DefaultElectionTimeoutMax,
			HeartbeatInterval:  DefaultHeartbeatInterval,
		},
	}
}

// WithPeers sets the peer IDs.
func (b *RaftConfigBuilder) WithPeers(peers []string) *RaftConfigBuilder {
	b.config.Peers = append([]string(nil), peers...)
	return b
}

// WithPeerAddrs sets the peer address mapping.
func (b *RaftConfigBuilder) WithPeerAddrs(addrs map[string]string) *RaftConfigBuilder {
	b.config.PeerAddrs = make(map[string]string, len(addrs))
	for k, v := range addrs {
		b.config.PeerAddrs[k] = v
	}
	return b
}

// WithStorage sets the storage implementation.
func (b *RaftConfigBuilder) WithStorage(s storage.Storage) *RaftConfigBuilder {
	b.config.Storage = s
	return b
}

// WithTransport sets the transport implementation.
func (b *RaftConfigBuilder) WithTransport(t transport.Transport) *RaftConfigBuilder {
	b.config.Transport = t
	return b
}

// WithApplyCh sets the apply channel.
func (b *RaftConfigBuilder) WithApplyCh(ch chan ApplyMsg) *RaftConfigBuilder {
	b.config.ApplyCh = ch
	return b
}

// WithElectionTimeout sets the election timeout range.
func (b *RaftConfigBuilder) WithElectionTimeout(min, max time.Duration) *RaftConfigBuilder {
	b.config.ElectionTimeoutMin = min
	b.config.ElectionTimeoutMax = max
	return b
}

// WithHeartbeatInterval sets the heartbeat interval.
func (b *RaftConfigBuilder) WithHeartbeatInterval(interval time.Duration) *RaftConfigBuilder {
	b.config.HeartbeatInterval = interval
	return b
}

// WithLogger sets the logger.
func (b *RaftConfigBuilder) WithLogger(logger *zap.Logger) *RaftConfigBuilder {
	b.config.Logger = logger
	return b
}

// Build creates the RaftConfig, returning an error if required fields are missing.
func (b *RaftConfigBuilder) Build() (*RaftConfig, error) {
	if b.config.ID == "" {
		return nil, errors.New("node ID is required")
	}
	if b.config.Storage == nil {
		return nil, errors.New("storage is required")
	}
	if b.config.Transport == nil {
		return nil, errors.New("transport is required")
	}
	if b.config.ApplyCh == nil {
		return nil, errors.New("apply channel is required")
	}
	if b.config.ElectionTimeoutMin <= 0 {
		return nil, errors.New("election timeout min must be positive")
	}
	if b.config.ElectionTimeoutMax <= b.config.ElectionTimeoutMin {
		return nil, errors.New("election timeout max must be greater than min")
	}
	if b.config.HeartbeatInterval <= 0 {
		return nil, errors.New("heartbeat interval must be positive")
	}
	if b.config.HeartbeatInterval >= b.config.ElectionTimeoutMin {
		return nil, errors.New("heartbeat interval must be less than election timeout min")
	}
	return b.config, nil
}

// MustBuild creates the RaftConfig, panicking if required fields are missing.
func (b *RaftConfigBuilder) MustBuild() *RaftConfig {
	cfg, err := b.Build()
	if err != nil {
		panic(err)
	}
	return cfg
}

// --------------------------------------------------------------------------
// Functional Options Pattern
// Alternative to builder for optional configuration
// --------------------------------------------------------------------------

// RaftOption is a functional option for configuring RaftNode.
type RaftOption func(*RaftConfig)

// WithPeersOption returns an option that sets the peer IDs.
func WithPeersOption(peers []string) RaftOption {
	return func(cfg *RaftConfig) {
		cfg.Peers = append([]string(nil), peers...)
	}
}

// WithPeerAddrsOption returns an option that sets peer addresses.
func WithPeerAddrsOption(addrs map[string]string) RaftOption {
	return func(cfg *RaftConfig) {
		cfg.PeerAddrs = make(map[string]string, len(addrs))
		for k, v := range addrs {
			cfg.PeerAddrs[k] = v
		}
	}
}

// WithStorageOption returns an option that sets the storage.
func WithStorageOption(s storage.Storage) RaftOption {
	return func(cfg *RaftConfig) {
		cfg.Storage = s
	}
}

// WithTransportOption returns an option that sets the transport.
func WithTransportOption(t transport.Transport) RaftOption {
	return func(cfg *RaftConfig) {
		cfg.Transport = t
	}
}

// WithApplyChOption returns an option that sets the apply channel.
func WithApplyChOption(ch chan ApplyMsg) RaftOption {
	return func(cfg *RaftConfig) {
		cfg.ApplyCh = ch
	}
}

// WithElectionTimeoutOption returns an option that sets the election timeout.
func WithElectionTimeoutOption(min, max time.Duration) RaftOption {
	return func(cfg *RaftConfig) {
		cfg.ElectionTimeoutMin = min
		cfg.ElectionTimeoutMax = max
	}
}

// WithHeartbeatIntervalOption returns an option that sets the heartbeat interval.
func WithHeartbeatIntervalOption(interval time.Duration) RaftOption {
	return func(cfg *RaftConfig) {
		cfg.HeartbeatInterval = interval
	}
}

// WithLoggerOption returns an option that sets the logger.
func WithLoggerOption(logger *zap.Logger) RaftOption {
	return func(cfg *RaftConfig) {
		cfg.Logger = logger
	}
}

// NewRaftConfig creates a new RaftConfig with the given ID and options.
func NewRaftConfig(id string, opts ...RaftOption) *RaftConfig {
	cfg := &RaftConfig{
		ID:                 id,
		ElectionTimeoutMin: DefaultElectionTimeoutMin,
		ElectionTimeoutMax: DefaultElectionTimeoutMax,
		HeartbeatInterval:  DefaultHeartbeatInterval,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}
