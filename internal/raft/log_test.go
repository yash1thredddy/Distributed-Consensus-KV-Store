package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yourusername/distributed-kv/internal/storage"
)

func TestLog_EmptyLog(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	log := NewLog(store)

	assert.Equal(t, int64(0), log.LastIndex())
	assert.Equal(t, int64(0), log.LastTerm())
	assert.Nil(t, log.GetEntry(1))
	assert.Equal(t, int64(0), log.GetTerm(0))
	assert.Equal(t, int64(0), log.GetTerm(1))
}

func TestLog_AppendAndGet(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	log := NewLog(store)

	// Append entries
	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 2, Index: 3, Command: []byte("cmd3")},
	}

	for _, entry := range entries {
		err := log.Append(entry)
		require.NoError(t, err)
	}

	// Verify state
	assert.Equal(t, int64(3), log.LastIndex())
	assert.Equal(t, int64(2), log.LastTerm())

	// Get individual entries
	for _, expected := range entries {
		entry := log.GetEntry(expected.Index)
		require.NotNil(t, entry)
		assert.Equal(t, expected.Term, entry.Term)
		assert.Equal(t, expected.Index, entry.Index)
		assert.Equal(t, expected.Command, entry.Command)
	}

	// GetTerm
	assert.Equal(t, int64(1), log.GetTerm(1))
	assert.Equal(t, int64(1), log.GetTerm(2))
	assert.Equal(t, int64(2), log.GetTerm(3))
}

func TestLog_GetEntries(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	log := NewLog(store)

	// Append 5 entries
	for i := 1; i <= 5; i++ {
		err := log.Append(LogEntry{
			Term:  int64(i),
			Index: int64(i),
		})
		require.NoError(t, err)
	}

	tests := []struct {
		name     string
		start    int64
		end      int64
		expected int
	}{
		{"full range", 1, 6, 5},
		{"partial range", 2, 4, 2},
		{"single entry", 3, 4, 1},
		{"empty range", 3, 3, 0},
		{"beyond end", 1, 10, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entries := log.GetEntries(tt.start, tt.end)
			assert.Len(t, entries, tt.expected)
		})
	}
}

func TestLog_TruncateAfter(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	log := NewLog(store)

	// Append 5 entries
	for i := 1; i <= 5; i++ {
		err := log.Append(LogEntry{
			Term:  1,
			Index: int64(i),
		})
		require.NoError(t, err)
	}

	assert.Equal(t, int64(5), log.LastIndex())

	// Truncate after index 3
	err := log.TruncateAfter(3)
	require.NoError(t, err)

	assert.Equal(t, int64(3), log.LastIndex())
	assert.NotNil(t, log.GetEntry(3))
	assert.Nil(t, log.GetEntry(4))
	assert.Nil(t, log.GetEntry(5))
}

func TestLog_TruncateAfter_NoOp(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	log := NewLog(store)

	// Append 3 entries
	for i := 1; i <= 3; i++ {
		err := log.Append(LogEntry{
			Term:  1,
			Index: int64(i),
		})
		require.NoError(t, err)
	}

	// Truncate after last index (no-op)
	err := log.TruncateAfter(3)
	require.NoError(t, err)
	assert.Equal(t, int64(3), log.LastIndex())

	// Truncate after index beyond last (no-op)
	err = log.TruncateAfter(10)
	require.NoError(t, err)
	assert.Equal(t, int64(3), log.LastIndex())
}

func TestLog_HasEntry(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	log := NewLog(store)

	err := log.Append(LogEntry{Term: 1, Index: 1})
	require.NoError(t, err)
	err = log.Append(LogEntry{Term: 2, Index: 2})
	require.NoError(t, err)

	// Index 0 with term 0 is the sentinel
	assert.True(t, log.HasEntry(0, 0))
	assert.False(t, log.HasEntry(0, 1))

	// Existing entries
	assert.True(t, log.HasEntry(1, 1))
	assert.False(t, log.HasEntry(1, 2))
	assert.True(t, log.HasEntry(2, 2))

	// Non-existent entry
	assert.False(t, log.HasEntry(3, 1))
}

func TestLog_AppendEntries(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	log := NewLog(store)

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 2, Index: 3, Command: []byte("cmd3")},
	}

	err := log.AppendEntries(entries)
	require.NoError(t, err)

	assert.Equal(t, int64(3), log.LastIndex())
	assert.Equal(t, int64(2), log.LastTerm())

	for _, expected := range entries {
		entry := log.GetEntry(expected.Index)
		require.NotNil(t, entry)
		assert.Equal(t, expected.Term, entry.Term)
	}
}

func TestLog_NextIndex(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	log := NewLog(store)

	assert.Equal(t, int64(1), log.NextIndex())

	err := log.Append(LogEntry{Term: 1, Index: 1})
	require.NoError(t, err)
	assert.Equal(t, int64(2), log.NextIndex())

	err = log.Append(LogEntry{Term: 1, Index: 2})
	require.NoError(t, err)
	assert.Equal(t, int64(3), log.NextIndex())
}

func TestLog_Persistence(t *testing.T) {
	store := storage.NewMemoryStorage()

	// Create log and add entries
	log1 := NewLog(store)
	for i := 1; i <= 3; i++ {
		err := log1.Append(LogEntry{
			Term:    int64(i),
			Index:   int64(i),
			Command: []byte("cmd"),
		})
		require.NoError(t, err)
	}

	// Create new log with same storage
	log2 := NewLog(store)

	// New log should restore lastIndex from storage
	assert.Equal(t, int64(3), log2.LastIndex())

	// Entries should be accessible
	for i := int64(1); i <= 3; i++ {
		entry := log2.GetEntry(i)
		require.NotNil(t, entry)
		assert.Equal(t, i, entry.Term)
	}

	store.Close()
}
