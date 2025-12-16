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

// --------------------------------------------------------------------------
// Interface Segregation: Small, focused interfaces for different consumers
// --------------------------------------------------------------------------

// KVStorage defines key-value operations for the state machine.
// Used by: KVServer
type KVStorage interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
}

// RaftStateStorage defines operations for Raft persistent state.
// Used by: RaftNode for term/votedFor persistence
type RaftStateStorage interface {
	SaveRaftState(state *PersistentState) error
	LoadRaftState() (*PersistentState, error)
}

// LogStorage defines operations for Raft log entries.
// Used by: Log for log entry management
type LogStorage interface {
	AppendLogEntries(entries []LogEntry) error
	GetLogEntry(index int64) (*LogEntry, error)
	GetLogEntries(startIndex, endIndex int64) ([]LogEntry, error)
	GetLastLogIndexAndTerm() (index int64, term int64, err error)
	TruncateLogAfter(index int64) error  // Delete entries after index
	TruncateLogBefore(index int64) error // Delete entries before index (after snapshot)
}

// SnapshotStorage defines operations for Raft snapshots.
// Used by: RaftNode for snapshotting
type SnapshotStorage interface {
	SaveSnapshot(index, term int64, data []byte) error
	LoadSnapshot() (index int64, term int64, data []byte, err error)
}

// Closer defines the lifecycle close operation.
type Closer interface {
	Close() error
}

// --------------------------------------------------------------------------
// Composite interfaces for components that need multiple capabilities
// --------------------------------------------------------------------------

// RaftStorage combines all storage operations needed by Raft.
// Used by: RaftNode (needs state, log, and snapshot operations)
type RaftStorage interface {
	RaftStateStorage
	LogStorage
	SnapshotStorage
}

// Storage is the full storage interface combining all capabilities.
// Implementations (MemoryStorage, BadgerStorage) implement this.
// This maintains backward compatibility with existing code.
type Storage interface {
	KVStorage
	RaftStateStorage
	LogStorage
	SnapshotStorage
	Closer
}
