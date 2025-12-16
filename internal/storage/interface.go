package storage

import (
	"errors"
)

// Common errors
var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrLogNotFound   = errors.New("log entry not found")
	ErrNoSnapshot    = errors.New("no snapshot available")
	ErrStorageClosed = errors.New("storage is closed")
)

// LogEntry represents a Raft log entry for storage.
type LogEntry struct {
	Term    int64  `json:"term"`
	Index   int64  `json:"index"`
	Command []byte `json:"command"`
}

// PersistentState represents Raft state that must survive restarts.
type PersistentState struct {
	CurrentTerm int64  `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

// Storage defines the interface for persistent storage operations.
// It handles both KV data and Raft state persistence.
type Storage interface {
	// KV operations (for state machine)
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error

	// Raft persistent state
	SaveRaftState(state *PersistentState) error
	LoadRaftState() (*PersistentState, error)

	// Raft log operations
	AppendLogEntries(entries []LogEntry) error
	GetLogEntry(index int64) (*LogEntry, error)
	GetLogEntries(startIndex, endIndex int64) ([]LogEntry, error)
	GetLastLogIndexAndTerm() (index int64, term int64, err error)
	TruncateLogAfter(index int64) error  // Delete entries after index
	TruncateLogBefore(index int64) error // Delete entries before index (after snapshot)

	// Snapshot operations
	SaveSnapshot(index, term int64, data []byte) error
	LoadSnapshot() (index int64, term int64, data []byte, err error)

	// Lifecycle
	Close() error
}
