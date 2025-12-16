package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/raft"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/storage"
)

// Errors
var (
	ErrNotLeader    = errors.New("not leader")
	ErrTimeout      = errors.New("operation timed out")
	ErrServerClosed = errors.New("server closed")
)

// DefaultOperationTimeout is the default timeout for operations.
const DefaultOperationTimeout = 5 * time.Second

// OpResult represents the result of an operation.
type OpResult struct {
	Value []byte
	Err   error
}

// KVServer is the key-value server that uses Raft for replication.
type KVServer struct {
	mu sync.RWMutex

	raft    *raft.RaftNode
	storage storage.KVStorage // Uses segregated interface (ISP) - only KV ops needed

	// pendingOps tracks pending operations by request ID (not log index).
	// Using request ID avoids a race condition: we can register the channel
	// BEFORE calling Propose(), so even if the entry is applied very quickly,
	// handleApplyMsg will find the channel.
	pendingOps map[uint64]chan OpResult

	// nextRequestID is an atomic counter for generating unique request IDs
	nextRequestID uint64

	// stopCh signals shutdown
	stopCh chan struct{}
	wg     sync.WaitGroup

	// operationTimeout is the timeout for client operations
	operationTimeout time.Duration
}

// KVServerConfig holds configuration for KVServer.
type KVServerConfig struct {
	Raft             *raft.RaftNode
	Storage          storage.KVStorage // Uses segregated interface (ISP)
	OperationTimeout time.Duration
}

// NewKVServer creates a new KV server.
// Panics if cfg, cfg.Raft, or cfg.Storage is nil.
func NewKVServer(cfg *KVServerConfig) *KVServer {
	if cfg == nil {
		panic("kv server config is nil")
	}
	if cfg.Raft == nil {
		panic("raft node is nil")
	}
	if cfg.Storage == nil {
		panic("storage is nil")
	}

	timeout := cfg.OperationTimeout
	if timeout == 0 {
		timeout = DefaultOperationTimeout
	}

	kv := &KVServer{
		raft:             cfg.Raft,
		storage:          cfg.Storage,
		pendingOps:       make(map[uint64]chan OpResult),
		stopCh:           make(chan struct{}),
		operationTimeout: timeout,
	}

	return kv
}

// Start starts the KV server's background goroutines.
func (kv *KVServer) Start() error {
	kv.wg.Add(1)
	go kv.applyLoop()
	return nil
}

// Stop stops the KV server.
func (kv *KVServer) Stop() error {
	// Cancel all pending operations BEFORE stopping goroutines.
	// This ensures in-flight requests get a response rather than timing out.
	kv.mu.Lock()
	for _, ch := range kv.pendingOps {
		select {
		case ch <- OpResult{Err: ErrServerClosed}:
		default:
		}
	}
	kv.pendingOps = make(map[uint64]chan OpResult)
	kv.mu.Unlock()

	// Now signal goroutines to stop and wait for them
	close(kv.stopCh)
	kv.wg.Wait()

	return nil
}

// applyLoop reads from the Raft apply channel and applies commands.
func (kv *KVServer) applyLoop() {
	defer kv.wg.Done()

	applyCh := kv.raft.ApplyCh()

	for {
		select {
		case <-kv.stopCh:
			return
		case msg, ok := <-applyCh:
			if !ok {
				return
			}
			kv.handleApplyMsg(msg)
		}
	}
}

// handleApplyMsg processes an applied message from Raft.
func (kv *KVServer) handleApplyMsg(msg raft.ApplyMsg) {
	if msg.SnapshotValid {
		// Handle snapshot restoration
		kv.applySnapshot(msg.Snapshot)
		return
	}

	if !msg.CommandValid {
		return
	}

	// Apply the command (already decoded by Raft layer)
	var result OpResult
	var requestID uint64

	if msg.Command == nil {
		result.Err = errors.New("nil command")
	} else {
		result = kv.applyCommand(msg.Command)
		requestID = msg.Command.RequestID
	}

	// Notify any waiting client using the request ID
	// This avoids the race condition of registering by log index after Propose
	if requestID != 0 {
		kv.mu.Lock()
		if ch, ok := kv.pendingOps[requestID]; ok {
			delete(kv.pendingOps, requestID)
			kv.mu.Unlock()

			select {
			case ch <- result:
			default:
			}
		} else {
			kv.mu.Unlock()
		}
	}
}

// applyCommand applies a command to the state machine.
func (kv *KVServer) applyCommand(cmd *raft.Command) OpResult {
	var result OpResult

	switch cmd.Type {
	case raft.CommandPut:
		err := kv.storage.Put([]byte(cmd.Key), cmd.Value)
		result.Err = err

	case raft.CommandDelete:
		err := kv.storage.Delete([]byte(cmd.Key))
		result.Err = err

	case raft.CommandNoop:
		// No-op, nothing to do

	default:
		result.Err = errors.New("unknown command type")
	}

	return result
}

// applySnapshot applies a snapshot to restore state.
func (kv *KVServer) applySnapshot(data []byte) {
	// For now, snapshots are handled by the storage layer directly
	// This would be used if we needed to do additional processing
}

// Get retrieves a value by key.
// If linearizable is true, ensures the read reflects all committed writes.
//
// IMPORTANT: The current linearizable read implementation provides "lease-less"
// consistency by checking leader status and waiting for applies to catch up.
// This provides strong consistency under normal operation but has a theoretical
// race window: the leader could lose leadership after the IsLeader check but
// before the read completes. For truly linearizable reads, consider:
//   - Read Index: Confirm leadership via heartbeat quorum before reading
//   - Lease-based: Use time-bound leader leases
//   - Log-based: Route reads through Raft log (expensive)
//
// The current implementation is suitable for most use cases where the brief
// race window is acceptable.
func (kv *KVServer) Get(ctx context.Context, key string, linearizable bool) ([]byte, bool, error) {
	if linearizable {
		// For linearizable reads, we need to ensure we're reading from a leader
		// that has committed all previous entries.
		// Note: This is a best-effort implementation. See function documentation
		// for limitations regarding the theoretical race window.
		if !kv.raft.IsLeader() {
			return nil, false, kv.notLeaderError()
		}

		// Wait for the current commit index to be applied
		// This ensures we see all committed writes
		commitIndex := kv.raft.CommitIndex()
		lastApplied := kv.raft.LastApplied()

		if lastApplied < commitIndex {
			// Wait for apply to catch up
			deadline := time.Now().Add(kv.operationTimeout)
			for time.Now().Before(deadline) {
				if kv.raft.LastApplied() >= commitIndex {
					break
				}
				select {
				case <-ctx.Done():
					return nil, false, ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}

			if kv.raft.LastApplied() < commitIndex {
				return nil, false, ErrTimeout
			}
		}
	}

	// Read from storage
	value, err := kv.storage.Get([]byte(key))
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return value, true, nil
}

// Put stores a key-value pair.
func (kv *KVServer) Put(ctx context.Context, key string, value []byte) error {
	return kv.propose(ctx, &raft.Command{
		Type:  raft.CommandPut,
		Key:   key,
		Value: value,
	})
}

// Delete removes a key.
func (kv *KVServer) Delete(ctx context.Context, key string) error {
	return kv.propose(ctx, &raft.Command{
		Type: raft.CommandDelete,
		Key:  key,
	})
}

// propose submits a command through Raft and waits for it to be applied.
func (kv *KVServer) propose(ctx context.Context, cmd *raft.Command) error {
	// Generate a unique request ID and assign it to the command.
	// This allows us to register the result channel BEFORE calling Propose,
	// avoiding the race condition where the entry is applied before we can
	// register by log index.
	requestID := atomic.AddUint64(&kv.nextRequestID, 1)
	cmd.RequestID = requestID

	// Create and register the result channel BEFORE proposing.
	// This ensures handleApplyMsg will find the channel even if the entry
	// is applied very quickly (e.g., single-node cluster).
	resultCh := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.pendingOps[requestID] = resultCh
	kv.mu.Unlock()

	// Encode the command (after setting RequestID)
	data, err := cmd.Encode()
	if err != nil {
		// Clean up on encode error
		kv.mu.Lock()
		delete(kv.pendingOps, requestID)
		kv.mu.Unlock()
		return err
	}

	// Propose to Raft
	_, _, err = kv.raft.Propose(data)
	if err != nil {
		// Clean up on propose error
		kv.mu.Lock()
		delete(kv.pendingOps, requestID)
		kv.mu.Unlock()

		// Check if it's a not-leader error
		var notLeaderErr *raft.ErrNotLeaderWithHint
		if errors.As(err, &notLeaderErr) {
			return &ErrNotLeaderWithHint{
				LeaderID:   notLeaderErr.LeaderID,
				LeaderAddr: notLeaderErr.LeaderAddr,
			}
		}
		return err
	}

	// Wait for the result or timeout
	select {
	case <-ctx.Done():
		// Clean up pending op
		kv.mu.Lock()
		delete(kv.pendingOps, requestID)
		kv.mu.Unlock()
		return ctx.Err()

	case <-kv.stopCh:
		return ErrServerClosed

	case <-time.After(kv.operationTimeout):
		// Clean up pending op
		kv.mu.Lock()
		delete(kv.pendingOps, requestID)
		kv.mu.Unlock()
		return ErrTimeout

	case result := <-resultCh:
		return result.Err
	}
}

// notLeaderError returns an error indicating this node is not the leader.
func (kv *KVServer) notLeaderError() error {
	leaderID := kv.raft.LeaderID()
	leaderAddr := ""
	if leaderID != "" {
		leaderAddr = kv.raft.GetPeerAddr(leaderID)
	}
	return &ErrNotLeaderWithHint{
		LeaderID:   leaderID,
		LeaderAddr: leaderAddr,
	}
}

// IsLeader returns true if this node is the Raft leader.
func (kv *KVServer) IsLeader() bool {
	return kv.raft.IsLeader()
}

// LeaderID returns the current leader's ID.
func (kv *KVServer) LeaderID() string {
	return kv.raft.LeaderID()
}

// ErrNotLeaderWithHint contains leader information for client redirects.
type ErrNotLeaderWithHint struct {
	LeaderID   string
	LeaderAddr string
}

func (e *ErrNotLeaderWithHint) Error() string {
	if e.LeaderID == "" {
		return "not leader, leader unknown"
	}
	return "not leader, leader is " + e.LeaderID
}
