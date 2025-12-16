package storage

import (
	"sync"
)

// MemoryStorage is an in-memory implementation of the Storage interface.
// It is primarily used for testing and development.
type MemoryStorage struct {
	mu sync.RWMutex

	// KV data
	data map[string][]byte

	// Raft persistent state
	raftState *PersistentState

	// Raft log entries (indexed by log index)
	logEntries []LogEntry
	firstIndex int64 // Index of first entry in logEntries slice

	// Snapshot data
	snapshotIndex int64
	snapshotTerm  int64
	snapshotData  []byte

	closed bool
}

// NewMemoryStorage creates a new in-memory storage instance.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data:       make(map[string][]byte),
		raftState:  &PersistentState{},
		logEntries: make([]LogEntry, 0),
		firstIndex: 1, // Log indices start at 1
	}
}

// Get retrieves a value by key.
func (m *MemoryStorage) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	value, ok := m.data[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Return a copy to prevent external modification
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Put stores a key-value pair.
func (m *MemoryStorage) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	// Store a copy to prevent external modification
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	m.data[string(key)] = valueCopy
	return nil
}

// Delete removes a key-value pair.
func (m *MemoryStorage) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	delete(m.data, string(key))
	return nil
}

// SaveRaftState persists the Raft state.
func (m *MemoryStorage) SaveRaftState(state *PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	m.raftState = &PersistentState{
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
	}
	return nil
}

// LoadRaftState retrieves the Raft state.
func (m *MemoryStorage) LoadRaftState() (*PersistentState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	if m.raftState == nil {
		return &PersistentState{}, nil
	}

	return &PersistentState{
		CurrentTerm: m.raftState.CurrentTerm,
		VotedFor:    m.raftState.VotedFor,
	}, nil
}

// AppendLogEntries appends entries to the log.
func (m *MemoryStorage) AppendLogEntries(entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	for _, entry := range entries {
		// Make a copy of the command
		cmdCopy := make([]byte, len(entry.Command))
		copy(cmdCopy, entry.Command)

		entryCopy := LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Command: cmdCopy,
		}

		// Calculate position in slice
		pos := entry.Index - m.firstIndex

		if pos < 0 {
			// Entry is before our first index, skip
			continue
		}

		if pos < int64(len(m.logEntries)) {
			// Overwrite existing entry
			m.logEntries[pos] = entryCopy
		} else if pos == int64(len(m.logEntries)) {
			// Append new entry
			m.logEntries = append(m.logEntries, entryCopy)
		} else {
			// Gap in indices - fill with empty entries (shouldn't happen in normal operation)
			for int64(len(m.logEntries)) < pos {
				m.logEntries = append(m.logEntries, LogEntry{})
			}
			m.logEntries = append(m.logEntries, entryCopy)
		}
	}

	return nil
}

// GetLogEntry retrieves a single log entry by index.
func (m *MemoryStorage) GetLogEntry(index int64) (*LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	if index < m.firstIndex {
		return nil, ErrLogNotFound
	}

	pos := index - m.firstIndex
	if pos >= int64(len(m.logEntries)) {
		return nil, ErrLogNotFound
	}

	entry := m.logEntries[pos]
	cmdCopy := make([]byte, len(entry.Command))
	copy(cmdCopy, entry.Command)

	return &LogEntry{
		Term:    entry.Term,
		Index:   entry.Index,
		Command: cmdCopy,
	}, nil
}

// GetLogEntries retrieves a range of log entries [startIndex, endIndex).
func (m *MemoryStorage) GetLogEntries(startIndex, endIndex int64) ([]LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	if startIndex >= endIndex {
		return []LogEntry{}, nil
	}

	if startIndex < m.firstIndex {
		startIndex = m.firstIndex
	}

	lastIndex := m.firstIndex + int64(len(m.logEntries))
	if endIndex > lastIndex {
		endIndex = lastIndex
	}

	if startIndex >= endIndex {
		return []LogEntry{}, nil
	}

	startPos := startIndex - m.firstIndex
	endPos := endIndex - m.firstIndex

	result := make([]LogEntry, 0, endPos-startPos)
	for i := startPos; i < endPos; i++ {
		entry := m.logEntries[i]
		cmdCopy := make([]byte, len(entry.Command))
		copy(cmdCopy, entry.Command)

		result = append(result, LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Command: cmdCopy,
		})
	}

	return result, nil
}

// GetLastLogIndexAndTerm returns the index and term of the last log entry.
func (m *MemoryStorage) GetLastLogIndexAndTerm() (index int64, term int64, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return 0, 0, ErrStorageClosed
	}

	if len(m.logEntries) == 0 {
		// No entries, return snapshot info if available
		if m.snapshotIndex > 0 {
			return m.snapshotIndex, m.snapshotTerm, nil
		}
		return 0, 0, nil
	}

	lastEntry := m.logEntries[len(m.logEntries)-1]
	return lastEntry.Index, lastEntry.Term, nil
}

// TruncateLogAfter removes all entries after the given index.
func (m *MemoryStorage) TruncateLogAfter(index int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	if index < m.firstIndex {
		// Truncate everything
		m.logEntries = m.logEntries[:0]
		return nil
	}

	pos := index - m.firstIndex + 1
	if pos < int64(len(m.logEntries)) {
		m.logEntries = m.logEntries[:pos]
	}

	return nil
}

// TruncateLogBefore removes all entries before the given index.
func (m *MemoryStorage) TruncateLogBefore(index int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	if index <= m.firstIndex {
		return nil
	}

	pos := index - m.firstIndex
	if pos >= int64(len(m.logEntries)) {
		// Remove all entries
		m.logEntries = m.logEntries[:0]
		m.firstIndex = index
		return nil
	}

	// Keep entries from pos onwards
	remaining := make([]LogEntry, len(m.logEntries)-int(pos))
	copy(remaining, m.logEntries[pos:])
	m.logEntries = remaining
	m.firstIndex = index

	return nil
}

// SaveSnapshot saves a snapshot.
func (m *MemoryStorage) SaveSnapshot(index, term int64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	m.snapshotIndex = index
	m.snapshotTerm = term

	m.snapshotData = make([]byte, len(data))
	copy(m.snapshotData, data)

	return nil
}

// LoadSnapshot loads the current snapshot.
func (m *MemoryStorage) LoadSnapshot() (index int64, term int64, data []byte, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return 0, 0, nil, ErrStorageClosed
	}

	if m.snapshotData == nil {
		return 0, 0, nil, ErrNoSnapshot
	}

	dataCopy := make([]byte, len(m.snapshotData))
	copy(dataCopy, m.snapshotData)

	return m.snapshotIndex, m.snapshotTerm, dataCopy, nil
}

// Close closes the storage.
func (m *MemoryStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.data = nil
	m.logEntries = nil
	m.snapshotData = nil

	return nil
}

// Ensure MemoryStorage implements Storage interface
var _ Storage = (*MemoryStorage)(nil)
