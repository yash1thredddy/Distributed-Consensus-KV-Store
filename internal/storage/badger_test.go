package storage

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestBadger(t *testing.T) (*BadgerStorage, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)

	cfg := DefaultBadgerConfig(tmpDir)
	s, err := NewBadgerStorage(cfg)
	require.NoError(t, err)

	cleanup := func() {
		s.Close()
		os.RemoveAll(tmpDir)
	}

	return s, cleanup
}

func createInMemoryBadger(t *testing.T) (*BadgerStorage, func()) {
	t.Helper()

	cfg := &BadgerConfig{
		InMemory:   true,
		SyncWrites: false,
	}
	s, err := NewBadgerStorage(cfg)
	require.NoError(t, err)

	cleanup := func() {
		s.Close()
	}

	return s, cleanup
}

func TestBadgerStorage_KVOperations(t *testing.T) {
	s, cleanup := createInMemoryBadger(t)
	defer cleanup()

	t.Run("Put and Get", func(t *testing.T) {
		err := s.Put([]byte("key1"), []byte("value1"))
		require.NoError(t, err)

		value, err := s.Get([]byte("key1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("value1"), value)
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		_, err := s.Get([]byte("nonexistent"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("Overwrite existing key", func(t *testing.T) {
		err := s.Put([]byte("key2"), []byte("value1"))
		require.NoError(t, err)

		err = s.Put([]byte("key2"), []byte("value2"))
		require.NoError(t, err)

		value, err := s.Get([]byte("key2"))
		require.NoError(t, err)
		assert.Equal(t, []byte("value2"), value)
	})

	t.Run("Delete key", func(t *testing.T) {
		err := s.Put([]byte("key3"), []byte("value3"))
		require.NoError(t, err)

		err = s.Delete([]byte("key3"))
		require.NoError(t, err)

		_, err = s.Get([]byte("key3"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("Delete non-existent key", func(t *testing.T) {
		err := s.Delete([]byte("nonexistent"))
		require.NoError(t, err) // Should not error
	})
}

func TestBadgerStorage_RaftState(t *testing.T) {
	s, cleanup := createInMemoryBadger(t)
	defer cleanup()

	t.Run("Save and Load", func(t *testing.T) {
		state := &PersistentState{
			CurrentTerm: 5,
			VotedFor:    "node1",
		}

		err := s.SaveRaftState(state)
		require.NoError(t, err)

		loaded, err := s.LoadRaftState()
		require.NoError(t, err)
		assert.Equal(t, int64(5), loaded.CurrentTerm)
		assert.Equal(t, "node1", loaded.VotedFor)
	})

	t.Run("Load empty state", func(t *testing.T) {
		s2, cleanup2 := createInMemoryBadger(t)
		defer cleanup2()

		loaded, err := s2.LoadRaftState()
		require.NoError(t, err)
		assert.Equal(t, int64(0), loaded.CurrentTerm)
		assert.Equal(t, "", loaded.VotedFor)
	})

	t.Run("Update state", func(t *testing.T) {
		state1 := &PersistentState{CurrentTerm: 1, VotedFor: "node1"}
		err := s.SaveRaftState(state1)
		require.NoError(t, err)

		state2 := &PersistentState{CurrentTerm: 2, VotedFor: "node2"}
		err = s.SaveRaftState(state2)
		require.NoError(t, err)

		loaded, err := s.LoadRaftState()
		require.NoError(t, err)
		assert.Equal(t, int64(2), loaded.CurrentTerm)
		assert.Equal(t, "node2", loaded.VotedFor)
	})
}

func TestBadgerStorage_LogOperations(t *testing.T) {
	s, cleanup := createInMemoryBadger(t)
	defer cleanup()

	t.Run("Append and Get single entry", func(t *testing.T) {
		entries := []LogEntry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
		}

		err := s.AppendLogEntries(entries)
		require.NoError(t, err)

		entry, err := s.GetLogEntry(1)
		require.NoError(t, err)
		assert.Equal(t, int64(1), entry.Term)
		assert.Equal(t, int64(1), entry.Index)
		assert.Equal(t, []byte("cmd1"), entry.Command)
	})

	t.Run("Append multiple entries", func(t *testing.T) {
		entries := []LogEntry{
			{Term: 1, Index: 2, Command: []byte("cmd2")},
			{Term: 2, Index: 3, Command: []byte("cmd3")},
		}

		err := s.AppendLogEntries(entries)
		require.NoError(t, err)

		for _, expected := range entries {
			entry, err := s.GetLogEntry(expected.Index)
			require.NoError(t, err)
			assert.Equal(t, expected.Term, entry.Term)
			assert.Equal(t, expected.Index, entry.Index)
			assert.Equal(t, expected.Command, entry.Command)
		}
	})

	t.Run("Get non-existent entry", func(t *testing.T) {
		_, err := s.GetLogEntry(999)
		assert.ErrorIs(t, err, ErrLogNotFound)
	})

	t.Run("GetLogEntries range", func(t *testing.T) {
		result, err := s.GetLogEntries(1, 4)
		require.NoError(t, err)
		require.Len(t, result, 3)
		assert.Equal(t, int64(1), result[0].Index)
		assert.Equal(t, int64(2), result[1].Index)
		assert.Equal(t, int64(3), result[2].Index)
	})

	t.Run("GetLogEntries empty range", func(t *testing.T) {
		result, err := s.GetLogEntries(1, 1)
		require.NoError(t, err)
		assert.Len(t, result, 0)
	})

	t.Run("GetLastLogIndexAndTerm", func(t *testing.T) {
		index, term, err := s.GetLastLogIndexAndTerm()
		require.NoError(t, err)
		assert.Equal(t, int64(3), index)
		assert.Equal(t, int64(2), term)
	})

	t.Run("GetLastLogIndexAndTerm empty log", func(t *testing.T) {
		s2, cleanup2 := createInMemoryBadger(t)
		defer cleanup2()

		index, term, err := s2.GetLastLogIndexAndTerm()
		require.NoError(t, err)
		assert.Equal(t, int64(0), index)
		assert.Equal(t, int64(0), term)
	})
}

func TestBadgerStorage_LogTruncation(t *testing.T) {
	t.Run("TruncateLogAfter", func(t *testing.T) {
		s, cleanup := createInMemoryBadger(t)
		defer cleanup()

		entries := []LogEntry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
			{Term: 1, Index: 2, Command: []byte("cmd2")},
			{Term: 2, Index: 3, Command: []byte("cmd3")},
			{Term: 2, Index: 4, Command: []byte("cmd4")},
		}

		err := s.AppendLogEntries(entries)
		require.NoError(t, err)

		// Truncate after index 2 (keep 1 and 2)
		err = s.TruncateLogAfter(2)
		require.NoError(t, err)

		// Entry 1 and 2 should exist
		_, err = s.GetLogEntry(1)
		require.NoError(t, err)
		_, err = s.GetLogEntry(2)
		require.NoError(t, err)

		// Entry 3 and 4 should not exist
		_, err = s.GetLogEntry(3)
		assert.ErrorIs(t, err, ErrLogNotFound)
		_, err = s.GetLogEntry(4)
		assert.ErrorIs(t, err, ErrLogNotFound)

		index, term, err := s.GetLastLogIndexAndTerm()
		require.NoError(t, err)
		assert.Equal(t, int64(2), index)
		assert.Equal(t, int64(1), term)
	})

	t.Run("TruncateLogBefore", func(t *testing.T) {
		s, cleanup := createInMemoryBadger(t)
		defer cleanup()

		entries := []LogEntry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
			{Term: 1, Index: 2, Command: []byte("cmd2")},
			{Term: 2, Index: 3, Command: []byte("cmd3")},
			{Term: 2, Index: 4, Command: []byte("cmd4")},
		}

		err := s.AppendLogEntries(entries)
		require.NoError(t, err)

		// Truncate before index 3 (keep 3 and 4)
		err = s.TruncateLogBefore(3)
		require.NoError(t, err)

		// Entry 1 and 2 should not exist
		_, err = s.GetLogEntry(1)
		assert.ErrorIs(t, err, ErrLogNotFound)
		_, err = s.GetLogEntry(2)
		assert.ErrorIs(t, err, ErrLogNotFound)

		// Entry 3 and 4 should exist
		entry, err := s.GetLogEntry(3)
		require.NoError(t, err)
		assert.Equal(t, int64(3), entry.Index)

		entry, err = s.GetLogEntry(4)
		require.NoError(t, err)
		assert.Equal(t, int64(4), entry.Index)
	})
}

func TestBadgerStorage_Snapshot(t *testing.T) {
	s, cleanup := createInMemoryBadger(t)
	defer cleanup()

	t.Run("Save and Load", func(t *testing.T) {
		data := []byte("snapshot data")
		err := s.SaveSnapshot(10, 2, data)
		require.NoError(t, err)

		index, term, loaded, err := s.LoadSnapshot()
		require.NoError(t, err)
		assert.Equal(t, int64(10), index)
		assert.Equal(t, int64(2), term)
		assert.Equal(t, data, loaded)
	})

	t.Run("Load no snapshot", func(t *testing.T) {
		s2, cleanup2 := createInMemoryBadger(t)
		defer cleanup2()

		_, _, _, err := s2.LoadSnapshot()
		assert.ErrorIs(t, err, ErrNoSnapshot)
	})

	t.Run("Overwrite snapshot", func(t *testing.T) {
		err := s.SaveSnapshot(20, 3, []byte("new snapshot"))
		require.NoError(t, err)

		index, term, data, err := s.LoadSnapshot()
		require.NoError(t, err)
		assert.Equal(t, int64(20), index)
		assert.Equal(t, int64(3), term)
		assert.Equal(t, []byte("new snapshot"), data)
	})

	t.Run("GetLastLogIndexAndTerm with snapshot only", func(t *testing.T) {
		s2, cleanup2 := createInMemoryBadger(t)
		defer cleanup2()

		// Save snapshot but no log entries
		err := s2.SaveSnapshot(10, 2, []byte("snapshot"))
		require.NoError(t, err)

		index, term, err := s2.GetLastLogIndexAndTerm()
		require.NoError(t, err)
		assert.Equal(t, int64(10), index)
		assert.Equal(t, int64(2), term)
	})
}

func TestBadgerStorage_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-persist-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Write data
	cfg := DefaultBadgerConfig(tmpDir)
	s1, err := NewBadgerStorage(cfg)
	require.NoError(t, err)

	err = s1.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	state := &PersistentState{CurrentTerm: 5, VotedFor: "node1"}
	err = s1.SaveRaftState(state)
	require.NoError(t, err)

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
	}
	err = s1.AppendLogEntries(entries)
	require.NoError(t, err)

	err = s1.SaveSnapshot(10, 2, []byte("snapshot"))
	require.NoError(t, err)

	// Close
	s1.Close()

	// Reopen and verify
	s2, err := NewBadgerStorage(cfg)
	require.NoError(t, err)
	defer s2.Close()

	value, err := s2.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), value)

	loadedState, err := s2.LoadRaftState()
	require.NoError(t, err)
	assert.Equal(t, int64(5), loadedState.CurrentTerm)
	assert.Equal(t, "node1", loadedState.VotedFor)

	entry, err := s2.GetLogEntry(1)
	require.NoError(t, err)
	assert.Equal(t, []byte("cmd1"), entry.Command)

	index, term, data, err := s2.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, int64(10), index)
	assert.Equal(t, int64(2), term)
	assert.Equal(t, []byte("snapshot"), data)
}

func TestBadgerStorage_Closed(t *testing.T) {
	s, _ := createInMemoryBadger(t)
	s.Close()

	_, err := s.Get([]byte("key"))
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = s.Put([]byte("key"), []byte("value"))
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = s.Delete([]byte("key"))
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = s.SaveRaftState(&PersistentState{})
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, err = s.LoadRaftState()
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = s.AppendLogEntries([]LogEntry{})
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, err = s.GetLogEntry(1)
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, err = s.GetLogEntries(1, 2)
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, _, err = s.GetLastLogIndexAndTerm()
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = s.TruncateLogAfter(1)
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = s.TruncateLogBefore(1)
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = s.SaveSnapshot(1, 1, []byte{})
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, _, _, err = s.LoadSnapshot()
	assert.ErrorIs(t, err, ErrStorageClosed)
}

func TestBadgerStorage_Concurrency(t *testing.T) {
	s, cleanup := createInMemoryBadger(t)
	defer cleanup()

	var wg sync.WaitGroup
	numGoroutines := 50

	// First, add some entries
	entries := make([]LogEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = LogEntry{Term: 1, Index: int64(i + 1), Command: []byte("cmd")}
	}
	err := s.AppendLogEntries(entries)
	require.NoError(t, err)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := []byte("key")
			value := []byte("value")
			_ = s.Put(key, value)
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, _ = s.Get([]byte("key"))
			_, _ = s.GetLogEntry(int64(n%100 + 1))
			_, _ = s.GetLogEntries(1, 50)
			_, _, _ = s.GetLastLogIndexAndTerm()
		}(i)
	}

	wg.Wait()
}

// TestBadgerStorage_LogKeyOrdering verifies that log keys are ordered correctly
func TestBadgerStorage_LogKeyOrdering(t *testing.T) {
	s, cleanup := createInMemoryBadger(t)
	defer cleanup()

	// Add entries out of order
	entries := []LogEntry{
		{Term: 1, Index: 100, Command: []byte("cmd100")},
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 10, Command: []byte("cmd10")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
	}

	err := s.AppendLogEntries(entries)
	require.NoError(t, err)

	// GetLogEntries should return in order
	result, err := s.GetLogEntries(1, 101)
	require.NoError(t, err)
	require.Len(t, result, 4)

	assert.Equal(t, int64(1), result[0].Index)
	assert.Equal(t, int64(2), result[1].Index)
	assert.Equal(t, int64(10), result[2].Index)
	assert.Equal(t, int64(100), result[3].Index)
}
