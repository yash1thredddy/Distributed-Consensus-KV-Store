package raft

import (
	"sync"

	"github.com/yourusername/distributed-kv/internal/storage"
)

// Log manages Raft log entries with persistence.
// Uses 1-based indexing to match the Raft paper.
// Index 0 is a virtual sentinel entry (never stored).
type Log struct {
	mu        sync.RWMutex
	storage   storage.Storage
	lastIndex int64 // Index of last entry (0 if empty)

	// Cache for recent entries (optional optimization)
	cache       map[int64]*LogEntry
	cacheSize   int
	firstCached int64
}

// NewLog creates a new Log backed by the given storage.
func NewLog(s storage.Storage) *Log {
	l := &Log{
		storage:   s,
		cache:     make(map[int64]*LogEntry),
		cacheSize: 1000, // Cache last 1000 entries
	}

	// Restore lastIndex from storage
	lastIndex, _, err := s.GetLastLogIndexAndTerm()
	if err == nil {
		l.lastIndex = lastIndex
	}

	return l
}

// LastIndex returns the index of the last log entry (0 if empty).
func (l *Log) LastIndex() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex
}

// LastTerm returns the term of the last log entry (0 if empty).
func (l *Log) LastTerm() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.lastIndex == 0 {
		return 0
	}

	entry := l.getEntryLocked(l.lastIndex)
	if entry == nil {
		return 0
	}
	return entry.Term
}

// GetTerm returns the term at the given index (0 if index is 0 or doesn't exist).
func (l *Log) GetTerm(index int64) int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 {
		return 0
	}

	entry := l.getEntryLocked(index)
	if entry == nil {
		return 0
	}
	return entry.Term
}

// GetEntry returns the entry at the given index, or nil if not found.
func (l *Log) GetEntry(index int64) *LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.getEntryLocked(index)
}

// getEntryLocked returns the entry at the given index.
// Must be called with mu held.
func (l *Log) getEntryLocked(index int64) *LogEntry {
	if index < 1 || index > l.lastIndex {
		return nil
	}

	// Check cache first
	if entry, ok := l.cache[index]; ok {
		return entry
	}

	// Load from storage
	entry, err := l.storage.GetLogEntry(index)
	if err != nil {
		return nil
	}

	// Cache the entry
	l.cacheEntry(entry)

	return entry
}

// cacheEntry adds an entry to the cache.
// Must be called with mu held (at least RLock for reads, Lock for modifications).
func (l *Log) cacheEntry(entry *LogEntry) {
	if entry == nil {
		return
	}

	l.cache[entry.Index] = entry

	// Evict old entries if cache is too large
	if len(l.cache) > l.cacheSize {
		// Find and remove oldest entries
		minIndex := entry.Index
		for idx := range l.cache {
			if idx < minIndex {
				minIndex = idx
			}
		}
		// Remove entries older than minIndex + cacheSize/2
		threshold := minIndex + int64(l.cacheSize/2)
		for idx := range l.cache {
			if idx < threshold {
				delete(l.cache, idx)
			}
		}
	}
}

// GetEntries returns entries in the range [start, end).
func (l *Log) GetEntries(start, end int64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if start >= end || start < 1 {
		return nil
	}

	if end > l.lastIndex+1 {
		end = l.lastIndex + 1
	}

	// Try to get from storage
	entries, err := l.storage.GetLogEntries(start, end)
	if err != nil {
		return nil
	}

	return entries
}

// Append adds a new entry to the log and persists it.
// The entry's Index should already be set correctly.
func (l *Log) Append(entry LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Persist to storage
	if err := l.storage.AppendLogEntries([]LogEntry{entry}); err != nil {
		return err
	}

	// Update lastIndex
	if entry.Index > l.lastIndex {
		l.lastIndex = entry.Index
	}

	// Cache the entry
	l.cacheEntry(&entry)

	return nil
}

// AppendEntries adds multiple entries to the log and persists them.
func (l *Log) AppendEntries(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Persist to storage
	if err := l.storage.AppendLogEntries(entries); err != nil {
		return err
	}

	// Update lastIndex
	lastEntry := entries[len(entries)-1]
	if lastEntry.Index > l.lastIndex {
		l.lastIndex = lastEntry.Index
	}

	// Cache entries
	for i := range entries {
		l.cacheEntry(&entries[i])
	}

	return nil
}

// TruncateAfter removes all entries after the given index.
func (l *Log) TruncateAfter(index int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index >= l.lastIndex {
		return nil
	}

	// Truncate in storage
	if err := l.storage.TruncateLogAfter(index); err != nil {
		return err
	}

	// Update lastIndex
	l.lastIndex = index

	// Clear cache entries after index
	for idx := range l.cache {
		if idx > index {
			delete(l.cache, idx)
		}
	}

	return nil
}

// HasEntry returns true if an entry exists at the given index with the given term.
func (l *Log) HasEntry(index, term int64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 {
		return term == 0
	}

	entry := l.getEntryLocked(index)
	if entry == nil {
		return false
	}
	return entry.Term == term
}

// findFirstIndexOfTerm finds the first index with the given term.
// Must be called with mu held.
func (l *Log) findFirstIndexOfTerm(term int64) int64 {
	// Binary search could be used here for large logs
	// For simplicity, linear scan from the beginning
	for i := int64(1); i <= l.lastIndex; i++ {
		entry := l.getEntryLocked(i)
		if entry != nil && entry.Term == term {
			return i
		}
	}
	return 1
}

// NextIndex returns the next available index for appending.
func (l *Log) NextIndex() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex + 1
}
