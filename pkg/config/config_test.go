package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, "node1", cfg.NodeID)
	assert.Equal(t, "./data", cfg.DataDir)
	assert.Equal(t, ":5000", cfg.RaftAddr)
	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Equal(t, 150*time.Millisecond, cfg.ElectionTimeoutMin)
	assert.Equal(t, 300*time.Millisecond, cfg.ElectionTimeoutMax)
	assert.Equal(t, 50*time.Millisecond, cfg.HeartbeatInterval)
	assert.Equal(t, int64(10000), cfg.SnapshotThreshold)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *Config
		wantError bool
		errorMsg  string
	}{
		{
			name:      "Valid config",
			cfg:       DefaultConfig(),
			wantError: false,
		},
		{
			name: "Empty node ID",
			cfg: &Config{
				NodeID:             "",
				DataDir:            "./data",
				RaftAddr:           ":5000",
				HTTPAddr:           ":8080",
				ElectionTimeoutMin: 150 * time.Millisecond,
				ElectionTimeoutMax: 300 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
				SnapshotThreshold:  10000,
			},
			wantError: true,
			errorMsg:  "node_id cannot be empty",
		},
		{
			name: "Empty data dir",
			cfg: &Config{
				NodeID:             "node1",
				DataDir:            "",
				RaftAddr:           ":5000",
				HTTPAddr:           ":8080",
				ElectionTimeoutMin: 150 * time.Millisecond,
				ElectionTimeoutMax: 300 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
				SnapshotThreshold:  10000,
			},
			wantError: true,
			errorMsg:  "data_dir cannot be empty",
		},
		{
			name: "Invalid election timeout",
			cfg: &Config{
				NodeID:             "node1",
				DataDir:            "./data",
				RaftAddr:           ":5000",
				HTTPAddr:           ":8080",
				ElectionTimeoutMin: 300 * time.Millisecond,
				ElectionTimeoutMax: 150 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
				SnapshotThreshold:  10000,
			},
			wantError: true,
			errorMsg:  "election_timeout_max must be greater than election_timeout_min",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLoadConfigWithDefaults(t *testing.T) {
	cfg, err := Load("")
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Should return defaults when no config file is provided
	assert.Equal(t, "node1", cfg.NodeID)
	assert.Equal(t, "./data", cfg.DataDir)
}

func TestLoadConfigWithEnvVars(t *testing.T) {
	// Set environment variables
	os.Setenv("RAFT_NODE_ID", "test-node")
	os.Setenv("RAFT_DATA_DIR", "/tmp/test-data")
	defer func() {
		os.Unsetenv("RAFT_NODE_ID")
		os.Unsetenv("RAFT_DATA_DIR")
	}()

	cfg, err := Load("")
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Environment variables should override defaults
	assert.Equal(t, "test-node", cfg.NodeID)
	assert.Equal(t, "/tmp/test-data", cfg.DataDir)
}
