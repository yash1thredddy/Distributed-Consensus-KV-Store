package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// Key prefixes for different data types
var (
	prefixKV        = []byte("kv:")
	prefixRaftLog   = []byte("log:")
	prefixRaftState = []byte("state:")
	prefixSnapshot  = []byte("snap:")
)

// Keys for raft state and snapshot
var (
	keyRaftState    = append(prefixRaftState, []byte("current")...)
	keySnapshotMeta = append(prefixSnapshot, []byte("meta")...)
	keySnapshotData = append(prefixSnapshot, []byte("data")...)
)

// BadgerStorage is a BadgerDB implementation of the Storage interface.
type BadgerStorage struct {
	mu     sync.RWMutex
	db     *badger.DB
	closed bool
}

// BadgerConfig holds configuration for BadgerDB
type BadgerConfig struct {
	DataDir     string
	InMemory    bool   // For testing
	SyncWrites  bool   // Enable sync writes for durability
	Logger      badger.Logger
}

// DefaultBadgerConfig returns default BadgerDB configuration
func DefaultBadgerConfig(dataDir string) *BadgerConfig {
	return &BadgerConfig{
		DataDir:    dataDir,
		InMemory:   false,
		SyncWrites: true,
	}
}

// NewBadgerStorage creates a new BadgerDB storage instance.
func NewBadgerStorage(cfg *BadgerConfig) (*BadgerStorage, error) {
	opts := badger.DefaultOptions(cfg.DataDir)

	if cfg.InMemory {
		opts = opts.WithInMemory(true)
	}

	opts = opts.WithSyncWrites(cfg.SyncWrites)

	// Reduce logging noise
	opts = opts.WithLogger(nil)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	return &BadgerStorage{
		db: db,
	}, nil
}

// makeKVKey creates a key for KV data
func makeKVKey(key []byte) []byte {
	return append(prefixKV, key...)
}

// makeLogKey creates a key for log entries (big-endian for proper ordering)
func makeLogKey(index int64) []byte {
	key := make([]byte, len(prefixRaftLog)+8)
	copy(key, prefixRaftLog)
	binary.BigEndian.PutUint64(key[len(prefixRaftLog):], uint64(index))
	return key
}

// parseLogKey extracts the index from a log key
func parseLogKey(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key[len(prefixRaftLog):]))
}

// Get retrieves a value by key.
func (s *BadgerStorage) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	var result []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(makeKVKey(key))
		if err == badger.ErrKeyNotFound {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}

		result, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// Put stores a key-value pair.
func (s *BadgerStorage) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(makeKVKey(key), value)
	})
}

// Delete removes a key-value pair.
func (s *BadgerStorage) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(makeKVKey(key))
	})
}

// SaveRaftState persists the Raft state.
func (s *BadgerStorage) SaveRaftState(state *PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal raft state: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyRaftState, data)
	})
}

// LoadRaftState retrieves the Raft state.
func (s *BadgerStorage) LoadRaftState() (*PersistentState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	var state PersistentState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyRaftState)
		if err == badger.ErrKeyNotFound {
			return nil // Return empty state
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &state)
		})
	})

	if err != nil {
		return nil, err
	}
	return &state, nil
}

// AppendLogEntries appends entries to the log.
func (s *BadgerStorage) AppendLogEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if len(entries) == 0 {
		return nil
	}

	return s.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			data, err := json.Marshal(entry)
			if err != nil {
				return fmt.Errorf("failed to marshal log entry: %w", err)
			}
			if err := txn.Set(makeLogKey(entry.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetLogEntry retrieves a single log entry by index.
func (s *BadgerStorage) GetLogEntry(index int64) (*LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	var entry LogEntry
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(makeLogKey(index))
		if err == badger.ErrKeyNotFound {
			return ErrLogNotFound
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &entry)
		})
	})

	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// GetLogEntries retrieves a range of log entries [startIndex, endIndex).
func (s *BadgerStorage) GetLogEntries(startIndex, endIndex int64) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	if startIndex >= endIndex {
		return []LogEntry{}, nil
	}

	var entries []LogEntry
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixRaftLog
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := makeLogKey(startIndex)
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// Check if we've passed the end
			idx := parseLogKey(key)
			if idx >= endIndex {
				break
			}

			var entry LogEntry
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &entry)
			})
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return entries, nil
}

// GetLastLogIndexAndTerm returns the index and term of the last log entry.
func (s *BadgerStorage) GetLastLogIndexAndTerm() (index int64, term int64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, 0, ErrStorageClosed
	}

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixRaftLog
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to the end of the log prefix range
		// For reverse iteration, we need to seek to a key that's after all log keys
		seekKey := make([]byte, len(prefixRaftLog)+8)
		copy(seekKey, prefixRaftLog)
		for i := len(prefixRaftLog); i < len(seekKey); i++ {
			seekKey[i] = 0xFF
		}

		it.Seek(seekKey)
		if !it.Valid() {
			// Try seeking to prefix
			it.Rewind()
		}

		if it.ValidForPrefix(prefixRaftLog) {
			item := it.Item()
			var entry LogEntry
			valErr := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &entry)
			})
			if valErr != nil {
				return valErr
			}
			index = entry.Index
			term = entry.Term
			return nil
		}

		// No log entries, check snapshot
		snapItem, snapErr := txn.Get(keySnapshotMeta)
		if snapErr == badger.ErrKeyNotFound {
			return nil // Return 0, 0
		}
		if snapErr != nil {
			return snapErr
		}

		return snapItem.Value(func(val []byte) error {
			var meta snapshotMeta
			if err := json.Unmarshal(val, &meta); err != nil {
				return err
			}
			index = meta.Index
			term = meta.Term
			return nil
		})
	})

	return index, term, err
}

// TruncateLogAfter removes all entries after the given index.
func (s *BadgerStorage) TruncateLogAfter(index int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixRaftLog
		it := txn.NewIterator(opts)
		defer it.Close()

		var keysToDelete [][]byte
		startKey := makeLogKey(index + 1)

		for it.Seek(startKey); it.ValidForPrefix(prefixRaftLog); it.Next() {
			keyCopy := make([]byte, len(it.Item().Key()))
			copy(keyCopy, it.Item().Key())
			keysToDelete = append(keysToDelete, keyCopy)
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// TruncateLogBefore removes all entries before the given index.
func (s *BadgerStorage) TruncateLogBefore(index int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixRaftLog
		it := txn.NewIterator(opts)
		defer it.Close()

		var keysToDelete [][]byte

		for it.Rewind(); it.ValidForPrefix(prefixRaftLog); it.Next() {
			key := it.Item().Key()
			idx := parseLogKey(key)
			if idx >= index {
				break
			}
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			keysToDelete = append(keysToDelete, keyCopy)
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// snapshotMeta holds snapshot metadata
type snapshotMeta struct {
	Index int64 `json:"index"`
	Term  int64 `json:"term"`
}

// SaveSnapshot saves a snapshot.
func (s *BadgerStorage) SaveSnapshot(index, term int64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	meta := snapshotMeta{Index: index, Term: term}
	metaData, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot meta: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(keySnapshotMeta, metaData); err != nil {
			return err
		}
		return txn.Set(keySnapshotData, data)
	})
}

// LoadSnapshot loads the current snapshot.
func (s *BadgerStorage) LoadSnapshot() (index int64, term int64, data []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, 0, nil, ErrStorageClosed
	}

	err = s.db.View(func(txn *badger.Txn) error {
		// Load metadata
		metaItem, metaErr := txn.Get(keySnapshotMeta)
		if metaErr == badger.ErrKeyNotFound {
			return ErrNoSnapshot
		}
		if metaErr != nil {
			return metaErr
		}

		var meta snapshotMeta
		if err := metaItem.Value(func(val []byte) error {
			return json.Unmarshal(val, &meta)
		}); err != nil {
			return err
		}

		// Load data
		dataItem, dataErr := txn.Get(keySnapshotData)
		if dataErr == badger.ErrKeyNotFound {
			return ErrNoSnapshot
		}
		if dataErr != nil {
			return dataErr
		}

		var dataErr2 error
		data, dataErr2 = dataItem.ValueCopy(nil)
		if dataErr2 != nil {
			return dataErr2
		}

		index = meta.Index
		term = meta.Term
		return nil
	})

	return index, term, data, err
}

// Close closes the storage.
func (s *BadgerStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	return s.db.Close()
}

// Sync forces a sync of the database to disk
func (s *BadgerStorage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	return s.db.Sync()
}

// RunGC runs garbage collection on the database
func (s *BadgerStorage) RunGC() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	return s.db.RunValueLogGC(0.5)
}

// Ensure BadgerStorage implements Storage interface
var _ Storage = (*BadgerStorage)(nil)
