package raft

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/storage"
)

// Type aliases for storage types (to avoid breaking imports)
type LogEntry = storage.LogEntry
type PersistentState = storage.PersistentState

// NodeState represents the state of a Raft node
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// String returns the string representation of NodeState
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// CommandType represents the type of command
type CommandType int

const (
	CommandPut CommandType = iota
	CommandDelete
	CommandNoop
)

// String returns the string representation of CommandType
func (t CommandType) String() string {
	switch t {
	case CommandPut:
		return "Put"
	case CommandDelete:
		return "Delete"
	case CommandNoop:
		return "Noop"
	default:
		return "Unknown"
	}
}

// Command represents a KV operation
type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value []byte      `json:"value,omitempty"`
}

// Encode serializes the command to JSON
func (c *Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// DecodeCommand deserializes a command from JSON
func DecodeCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("failed to decode command: %w", err)
	}
	return &cmd, nil
}

// ApplyMsg is sent to the state machine when an entry commits
type ApplyMsg struct {
	// For committed commands
	CommandValid bool
	Command      *Command
	CommandIndex int64
	CommandTerm  int64

	// For snapshots
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int64
	SnapshotIndex int64
}

// Timing constants
const (
	// RPCTimeout is the timeout for RPC calls
	RPCTimeout = 100 * time.Millisecond

	// DefaultElectionTimeoutMin is the minimum election timeout
	DefaultElectionTimeoutMin = 150 * time.Millisecond

	// DefaultElectionTimeoutMax is the maximum election timeout
	DefaultElectionTimeoutMax = 300 * time.Millisecond

	// DefaultHeartbeatInterval is the interval between heartbeats
	DefaultHeartbeatInterval = 50 * time.Millisecond
)
