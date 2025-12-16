package testutil

import (
	"time"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/raft"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/storage"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/transport"
)

// TestNode wraps a RaftNode with its infrastructure for testing.
type TestNode struct {
	ID           string
	Node         *raft.RaftNode
	Storage      storage.Storage
	Transport    *transport.GRPCTransport
	Addr         string
	ApplyCh      chan raft.ApplyMsg
	storageOwned bool // true if we created the storage and should close it
}

// TestNodeConfig holds configuration for creating a test node.
type TestNodeConfig struct {
	ID                 string
	Addr               string
	Peers              []string
	PeerAddrs          map[string]string
	Storage            storage.Storage            // Optional: use existing storage
	ApplyCh            chan raft.ApplyMsg         // Optional: use existing channel
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

// DefaultTestNodeConfig returns default timing configuration for tests.
func DefaultTestNodeConfig() TestNodeConfig {
	return TestNodeConfig{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
}

// NewTestNode creates a new test node with the given configuration.
func NewTestNode(cfg TestNodeConfig) (*TestNode, error) {
	// Use provided storage or create new in-memory storage
	store := cfg.Storage
	storageOwned := store == nil
	if storageOwned {
		store = storage.NewMemoryStorage()
	}

	// Create transport
	transportCfg := transport.DefaultGRPCTransportConfig(cfg.Addr)
	trans := transport.NewGRPCTransport(transportCfg)

	// Use provided apply channel or create new one
	applyCh := cfg.ApplyCh
	if applyCh == nil {
		applyCh = make(chan raft.ApplyMsg, 100)
	}

	// Set default timeouts if not specified
	electionMin := cfg.ElectionTimeoutMin
	if electionMin == 0 {
		electionMin = 150 * time.Millisecond
	}
	electionMax := cfg.ElectionTimeoutMax
	if electionMax == 0 {
		electionMax = 300 * time.Millisecond
	}
	heartbeat := cfg.HeartbeatInterval
	if heartbeat == 0 {
		heartbeat = 50 * time.Millisecond
	}

	// Create RaftNode
	raftCfg := &raft.RaftConfig{
		ID:                 cfg.ID,
		Peers:              cfg.Peers,
		PeerAddrs:          cfg.PeerAddrs,
		Storage:            store,
		Transport:          trans,
		ApplyCh:            applyCh,
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  heartbeat,
	}

	node, err := raft.NewRaftNode(raftCfg)
	if err != nil {
		// Clean up transport on RaftNode creation failure
		trans.Stop()
		if storageOwned {
			store.Close()
		}
		return nil, err
	}

	return &TestNode{
		ID:           cfg.ID,
		Node:         node,
		Storage:      store,
		Transport:    trans,
		Addr:         cfg.Addr,
		ApplyCh:      applyCh,
		storageOwned: storageOwned,
	}, nil
}

// Start starts the test node (transport and raft).
func (n *TestNode) Start() error {
	if err := n.Transport.Start(); err != nil {
		return err
	}
	if err := n.Node.Start(); err != nil {
		// Clean up transport if node start fails
		n.Transport.Stop()
		return err
	}
	return nil
}

// Stop stops the test node.
func (n *TestNode) Stop() {
	n.Node.Stop()
	n.Transport.Stop()
}

// Close stops and cleans up all resources.
// Only closes storage if it was created by this TestNode.
func (n *TestNode) Close() {
	n.Stop()
	if n.storageOwned {
		n.Storage.Close()
	}
}

// IsLeader returns true if this node is the leader.
func (n *TestNode) IsLeader() bool {
	return n.Node.State() == raft.Leader
}

// IsFollower returns true if this node is a follower.
func (n *TestNode) IsFollower() bool {
	return n.Node.State() == raft.Follower
}

// State returns the current raft state.
func (n *TestNode) State() raft.NodeState {
	return n.Node.State()
}

// Term returns the current term.
func (n *TestNode) Term() int64 {
	return n.Node.Term()
}

// CommitIndex returns the current commit index.
func (n *TestNode) CommitIndex() int64 {
	return n.Node.CommitIndex()
}
