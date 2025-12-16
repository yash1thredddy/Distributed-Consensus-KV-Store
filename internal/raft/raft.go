package raft

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/api/proto/raftpb"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/metrics"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/storage"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/transport"
)

// Errors
var (
	ErrNotLeader     = errors.New("not leader")
	ErrStopped       = errors.New("raft node stopped")
	ErrProposalFailed = errors.New("proposal failed")
)

// ErrNotLeaderWithHint contains leader information for redirects
type ErrNotLeaderWithHint struct {
	LeaderID   string
	LeaderAddr string
}

func (e *ErrNotLeaderWithHint) Error() string {
	if e.LeaderID == "" {
		return "not leader, leader unknown"
	}
	return fmt.Sprintf("not leader, leader is %s", e.LeaderID)
}

// RaftNode represents a node in the Raft cluster.
type RaftNode struct {
	// mu protects all state below. Lock order: RaftNode.mu -> Log.mu -> Storage
	mu sync.RWMutex

	// Identity
	id    string   // This node's unique ID
	peers []string // IDs of other nodes (not including self)

	// Peer address mapping (peer ID -> address)
	peerAddrs map[string]string

	// Persistent state (MUST survive restart)
	currentTerm int64  // Latest term seen
	votedFor    string // CandidateId voted for in current term (empty if none)
	log         *Log   // Log entries

	// Volatile state (all servers)
	commitIndex int64 // Highest log entry known to be committed
	lastApplied int64 // Highest log entry applied to state machine

	// Volatile state (leaders only, reinitialized after election)
	nextIndex  map[string]int64 // For each peer: next log index to send
	matchIndex map[string]int64 // For each peer: highest log index known to be replicated

	// Runtime state
	state       NodeState // Follower, Candidate, or Leader
	leaderId    string    // Current known leader (for redirects)
	lastContact time.Time // Last time heard from leader/granted vote

	// Infrastructure
	transport transport.Transport
	storage   storage.Storage
	applyCh   chan ApplyMsg // Sends committed entries to state machine
	logger    *zap.Logger   // Structured logger

	// Goroutine coordination
	stopCh       chan struct{}   // Signals shutdown
	resetTimerCh chan struct{}   // Signals election timer reset
	newEntryCh   chan struct{}   // Signals new log entry for replication
	stepDownCh   chan struct{}   // Signals leader to step down
	wg           sync.WaitGroup  // Waits for goroutines to finish

	// Configuration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	// State
	started bool
}

// RaftConfig holds configuration for a RaftNode.
type RaftConfig struct {
	ID                 string
	Peers              []string          // Peer IDs (not including self)
	PeerAddrs          map[string]string // Peer ID -> address mapping
	Storage            storage.Storage
	Transport          transport.Transport
	ApplyCh            chan ApplyMsg
	Logger             *zap.Logger // Optional; defaults to no-op logger
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

// DefaultRaftConfig returns default configuration for RaftNode.
func DefaultRaftConfig(id string, peers []string, peerAddrs map[string]string, storage storage.Storage, transport transport.Transport) *RaftConfig {
	return &RaftConfig{
		ID:                 id,
		Peers:              peers,
		PeerAddrs:          peerAddrs,
		Storage:            storage,
		Transport:          transport,
		ApplyCh:            make(chan ApplyMsg, 100),
		ElectionTimeoutMin: DefaultElectionTimeoutMin,
		ElectionTimeoutMax: DefaultElectionTimeoutMax,
		HeartbeatInterval:  DefaultHeartbeatInterval,
	}
}

// NewRaftNode creates a new RaftNode.
func NewRaftNode(cfg *RaftConfig) (*RaftNode, error) {
	if cfg.ID == "" {
		return nil, errors.New("node ID cannot be empty")
	}
	if cfg.Storage == nil {
		return nil, errors.New("storage cannot be nil")
	}
	if cfg.Transport == nil {
		return nil, errors.New("transport cannot be nil")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	rn := &RaftNode{
		id:                 cfg.ID,
		peers:              cfg.Peers,
		peerAddrs:          cfg.PeerAddrs,
		storage:            cfg.Storage,
		transport:          cfg.Transport,
		applyCh:            cfg.ApplyCh,
		logger:             logger.With(zap.String("nodeId", cfg.ID)),
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		state:              Follower,
		stopCh:             make(chan struct{}),
		resetTimerCh:       make(chan struct{}, 1),
		newEntryCh:         make(chan struct{}, 1),
		stepDownCh:         make(chan struct{}, 1),
		nextIndex:          make(map[string]int64),
		matchIndex:         make(map[string]int64),
	}

	if rn.applyCh == nil {
		rn.applyCh = make(chan ApplyMsg, 100)
	}

	// Create log backed by storage
	rn.log = NewLog(cfg.Storage)

	// Restore persistent state from storage
	if err := rn.restoreState(); err != nil {
		return nil, fmt.Errorf("failed to restore state: %w", err)
	}

	return rn, nil
}

// restoreState restores persistent state from storage.
func (rn *RaftNode) restoreState() error {
	state, err := rn.storage.LoadRaftState()
	if err != nil {
		return err
	}
	if state != nil {
		rn.currentTerm = state.CurrentTerm
		rn.votedFor = state.VotedFor
	}

	// Restore log indices
	lastIndex, _, err := rn.storage.GetLastLogIndexAndTerm()
	if err != nil {
		return err
	}
	rn.log.lastIndex = lastIndex

	// Initialize metrics with restored state
	metrics.RaftCurrentTerm.Set(float64(rn.currentTerm))
	metrics.RaftState.WithLabelValues(rn.id).Set(float64(Follower))
	metrics.RaftIsLeader.Set(0)
	metrics.RaftCommitIndex.Set(float64(rn.commitIndex))
	metrics.RaftLastApplied.Set(float64(rn.lastApplied))
	metrics.RaftLogEntries.Set(float64(lastIndex))

	return nil
}

// persistState saves persistent state to storage.
func (rn *RaftNode) persistState() error {
	state := &PersistentState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
	}
	return rn.storage.SaveRaftState(state)
}

// Start starts the RaftNode's background goroutines.
func (rn *RaftNode) Start() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.started {
		return errors.New("raft node already started")
	}
	rn.started = true

	// Register as RPC handler
	rn.transport.RegisterRaftHandler(rn)

	// Start background goroutines
	rn.wg.Add(2)
	go rn.electionTimer()
	go rn.applyLoop()

	return nil
}

// Stop stops the RaftNode and waits for goroutines to finish.
func (rn *RaftNode) Stop() error {
	rn.mu.Lock()
	if !rn.started {
		rn.mu.Unlock()
		return nil
	}
	rn.started = false

	// Close stopCh only once - check if it's already closed
	select {
	case <-rn.stopCh:
		// Already closed
	default:
		close(rn.stopCh)
	}
	rn.mu.Unlock()

	rn.wg.Wait()

	return nil
}

// State returns the current state of the node.
func (rn *RaftNode) State() NodeState {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state
}

// Term returns the current term.
func (rn *RaftNode) Term() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentTerm
}

// LeaderID returns the current leader ID.
func (rn *RaftNode) LeaderID() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.leaderId
}

// ID returns this node's ID.
func (rn *RaftNode) ID() string {
	return rn.id
}

// CommitIndex returns the current commit index.
func (rn *RaftNode) CommitIndex() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.commitIndex
}

// LastApplied returns the last applied index.
func (rn *RaftNode) LastApplied() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.lastApplied
}

// ApplyCh returns the channel for applied entries.
func (rn *RaftNode) ApplyCh() <-chan ApplyMsg {
	return rn.applyCh
}

// IsLeader returns true if this node is the leader.
func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state == Leader
}

// GetPeerAddr returns the address for a peer ID.
func (rn *RaftNode) GetPeerAddr(peerID string) string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.peerAddrs[peerID]
}

// quorumSize returns the size needed for a quorum (majority).
func (rn *RaftNode) quorumSize() int {
	// Total nodes = peers + self
	totalNodes := len(rn.peers) + 1
	return totalNodes/2 + 1
}

// resetElectionTimer signals the election timer to reset.
// Non-blocking - if there's already a signal pending, this is a no-op.
func (rn *RaftNode) resetElectionTimer() {
	select {
	case rn.resetTimerCh <- struct{}{}:
	default:
	}
}

// triggerReplication signals that there are new entries to replicate.
// Non-blocking.
func (rn *RaftNode) triggerReplication() {
	select {
	case rn.newEntryCh <- struct{}{}:
	default:
	}
}

// signalStepDown signals the leader loop to stop.
// Non-blocking.
func (rn *RaftNode) signalStepDown() {
	select {
	case rn.stepDownCh <- struct{}{}:
	default:
	}
}

// stepDown transitions to follower and updates term.
// Must be called with mu held.
func (rn *RaftNode) stepDown(newTerm int64) {
	wasLeader := rn.state == Leader
	rn.currentTerm = newTerm
	rn.votedFor = ""
	rn.state = Follower

	// Update metrics
	metrics.RaftCurrentTerm.Set(float64(newTerm))
	metrics.RaftState.WithLabelValues(rn.id).Set(float64(Follower))
	metrics.RaftIsLeader.Set(0)
	if wasLeader {
		metrics.RaftLeaderChanges.Inc()
	}

	// Persist state
	if err := rn.persistState(); err != nil {
		// Log error but continue - we're already stepping down
	}

	// Signal leader loop to stop if we were leader
	rn.signalStepDown()
}

// convertLogEntriesToProto converts internal LogEntry slice to protobuf.
func convertLogEntriesToProto(entries []LogEntry) []*raftpb.LogEntry {
	result := make([]*raftpb.LogEntry, len(entries))
	for i, e := range entries {
		result[i] = &raftpb.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		}
	}
	return result
}

// convertProtoToLogEntries converts protobuf LogEntry slice to internal type.
func convertProtoToLogEntries(entries []*raftpb.LogEntry) []LogEntry {
	result := make([]LogEntry, len(entries))
	for i, e := range entries {
		result[i] = LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		}
	}
	return result
}

// isLogUpToDate checks if candidate's log is at least as up-to-date as ours.
// Comparison: term first, then index.
func (rn *RaftNode) isLogUpToDate(lastLogIndex, lastLogTerm int64) bool {
	myLastIndex := rn.log.LastIndex()
	myLastTerm := rn.log.LastTerm()

	// Term takes priority
	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	// Same term, compare index
	return lastLogIndex >= myLastIndex
}
