package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStorage_KVOperations(t *testing.T) {
	t.Run("Put and Get", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		err := s.Put([]byte("key1"), []byte("value1"))
		require.NoError(t, err)

		value, err := s.Get([]byte("key1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("value1"), value)
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		_, err := s.Get([]byte("nonexistent"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("Overwrite existing key", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		err := s.Put([]byte("key1"), []byte("value1"))
		require.NoError(t, err)

		err = s.Put([]byte("key1"), []byte("value2"))
		require.NoError(t, err)

		value, err := s.Get([]byte("key1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("value2"), value)
	})

	t.Run("Delete key", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		err := s.Put([]byte("key1"), []byte("value1"))
		require.NoError(t, err)

		err = s.Delete([]byte("key1"))
		require.NoError(t, err)

		_, err = s.Get([]byte("key1"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("Delete non-existent key", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		err := s.Delete([]byte("nonexistent"))
		require.NoError(t, err) // Should not error
	})

	t.Run("Get returns copy", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		err := s.Put([]byte("key1"), []byte("value1"))
		require.NoError(t, err)

		value, err := s.Get([]byte("key1"))
		require.NoError(t, err)

		// Modify the returned value
		value[0] = 'X'

		// Original should be unchanged
		value2, err := s.Get([]byte("key1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("value1"), value2)
	})
}

func TestMemoryStorage_RaftState(t *testing.T) {
	t.Run("Save and Load", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

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
		s := NewMemoryStorage()
		defer s.Close()

		loaded, err := s.LoadRaftState()
		require.NoError(t, err)
		assert.Equal(t, int64(0), loaded.CurrentTerm)
		assert.Equal(t, "", loaded.VotedFor)
	})

	t.Run("Update state", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

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

func TestMemoryStorage_LogOperations(t *testing.T) {
	t.Run("Append and Get single entry", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

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
		s := NewMemoryStorage()
		defer s.Close()

		entries := []LogEntry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
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
		s := NewMemoryStorage()
		defer s.Close()

		_, err := s.GetLogEntry(1)
		assert.ErrorIs(t, err, ErrLogNotFound)
	})

	t.Run("GetLogEntries range", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		entries := []LogEntry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
			{Term: 1, Index: 2, Command: []byte("cmd2")},
			{Term: 2, Index: 3, Command: []byte("cmd3")},
			{Term: 2, Index: 4, Command: []byte("cmd4")},
		}

		err := s.AppendLogEntries(entries)
		require.NoError(t, err)

		// Get entries [2, 4) - should return indices 2 and 3
		result, err := s.GetLogEntries(2, 4)
		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, int64(2), result[0].Index)
		assert.Equal(t, int64(3), result[1].Index)
	})

	t.Run("GetLogEntries empty range", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		result, err := s.GetLogEntries(1, 1)
		require.NoError(t, err)
		assert.Len(t, result, 0)
	})

	t.Run("GetLastLogIndexAndTerm empty log", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		index, term, err := s.GetLastLogIndexAndTerm()
		require.NoError(t, err)
		assert.Equal(t, int64(0), index)
		assert.Equal(t, int64(0), term)
	})

	t.Run("GetLastLogIndexAndTerm with entries", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		entries := []LogEntry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
			{Term: 2, Index: 2, Command: []byte("cmd2")},
			{Term: 3, Index: 3, Command: []byte("cmd3")},
		}

		err := s.AppendLogEntries(entries)
		require.NoError(t, err)

		index, term, err := s.GetLastLogIndexAndTerm()
		require.NoError(t, err)
		assert.Equal(t, int64(3), index)
		assert.Equal(t, int64(3), term)
	})
}

func TestMemoryStorage_LogTruncation(t *testing.T) {
	t.Run("TruncateLogAfter", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

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
		s := NewMemoryStorage()
		defer s.Close()

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

func TestMemoryStorage_Snapshot(t *testing.T) {
	t.Run("Save and Load", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

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
		s := NewMemoryStorage()
		defer s.Close()

		_, _, _, err := s.LoadSnapshot()
		assert.ErrorIs(t, err, ErrNoSnapshot)
	})

	t.Run("Overwrite snapshot", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		err := s.SaveSnapshot(10, 2, []byte("old"))
		require.NoError(t, err)

		err = s.SaveSnapshot(20, 3, []byte("new"))
		require.NoError(t, err)

		index, term, data, err := s.LoadSnapshot()
		require.NoError(t, err)
		assert.Equal(t, int64(20), index)
		assert.Equal(t, int64(3), term)
		assert.Equal(t, []byte("new"), data)
	})

	t.Run("GetLastLogIndexAndTerm with snapshot", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		// Save snapshot but no log entries
		err := s.SaveSnapshot(10, 2, []byte("snapshot"))
		require.NoError(t, err)

		index, term, err := s.GetLastLogIndexAndTerm()
		require.NoError(t, err)
		assert.Equal(t, int64(10), index)
		assert.Equal(t, int64(2), term)
	})
}

func TestMemoryStorage_Closed(t *testing.T) {
	t.Run("Operations after close", func(t *testing.T) {
		s := NewMemoryStorage()
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
	})
}

func TestMemoryStorage_Concurrency(t *testing.T) {
	t.Run("Concurrent KV operations", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		var wg sync.WaitGroup
		numGoroutines := 100

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
			go func() {
				defer wg.Done()
				_, _ = s.Get([]byte("key"))
			}()
		}

		wg.Wait()
	})

	t.Run("Concurrent log operations", func(t *testing.T) {
		s := NewMemoryStorage()
		defer s.Close()

		var wg sync.WaitGroup
		numGoroutines := 50

		// First, add some entries
		entries := make([]LogEntry, 100)
		for i := 0; i < 100; i++ {
			entries[i] = LogEntry{Term: 1, Index: int64(i + 1), Command: []byte("cmd")}
		}
		_ = s.AppendLogEntries(entries)

		// Concurrent reads
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				_, _ = s.GetLogEntry(int64(n%100 + 1))
				_, _ = s.GetLogEntries(1, 50)
				_, _, _ = s.GetLastLogIndexAndTerm()
			}(i)
		}

		wg.Wait()
	})
}
