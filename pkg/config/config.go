package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config represents the configuration for a Raft node
type Config struct {
	// NodeID is the unique identifier for this node
	NodeID string

	// DataDir is the base directory for all data
	DataDir string

	// RaftAddr is the address for Raft RPC (e.g., ":5000")
	RaftAddr string

	// HTTPAddr is the address for HTTP API (e.g., ":8080")
	HTTPAddr string

	// Peers is the list of peer addresses
	Peers []string

	// ElectionTimeoutMin is the minimum election timeout
	ElectionTimeoutMin time.Duration

	// ElectionTimeoutMax is the maximum election timeout
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is the interval between heartbeats
	HeartbeatInterval time.Duration

	// SnapshotThreshold is the number of log entries before snapshot
	SnapshotThreshold int64
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		NodeID:             "node1",
		DataDir:            "./data",
		RaftAddr:           ":5000",
		HTTPAddr:           ":8080",
		Peers:              []string{},
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		SnapshotThreshold:  10000,
	}
}

// Load loads configuration from a file with environment variable overrides
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path != "" {
		viper.SetConfigFile(path)
		if err := viper.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Set environment variable prefix
	viper.SetEnvPrefix("RAFT")
	viper.AutomaticEnv()

	// Bind configuration fields
	if viper.IsSet("node_id") {
		cfg.NodeID = viper.GetString("node_id")
	}
	if viper.IsSet("data_dir") {
		cfg.DataDir = viper.GetString("data_dir")
	}
	if viper.IsSet("raft_addr") {
		cfg.RaftAddr = viper.GetString("raft_addr")
	}
	if viper.IsSet("http_addr") {
		cfg.HTTPAddr = viper.GetString("http_addr")
	}
	if viper.IsSet("peers") {
		cfg.Peers = viper.GetStringSlice("peers")
	}
	if viper.IsSet("election_timeout_min") {
		cfg.ElectionTimeoutMin = viper.GetDuration("election_timeout_min")
	}
	if viper.IsSet("election_timeout_max") {
		cfg.ElectionTimeoutMax = viper.GetDuration("election_timeout_max")
	}
	if viper.IsSet("heartbeat_interval") {
		cfg.HeartbeatInterval = viper.GetDuration("heartbeat_interval")
	}
	if viper.IsSet("snapshot_threshold") {
		cfg.SnapshotThreshold = viper.GetInt64("snapshot_threshold")
	}

	return cfg, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node_id cannot be empty")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data_dir cannot be empty")
	}
	if c.RaftAddr == "" {
		return fmt.Errorf("raft_addr cannot be empty")
	}
	if c.HTTPAddr == "" {
		return fmt.Errorf("http_addr cannot be empty")
	}
	if c.ElectionTimeoutMin <= 0 {
		return fmt.Errorf("election_timeout_min must be positive")
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return fmt.Errorf("election_timeout_max must be greater than election_timeout_min")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat_interval must be positive")
	}
	if c.SnapshotThreshold <= 0 {
		return fmt.Errorf("snapshot_threshold must be positive")
	}
	return nil
}
