package testutil

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yourusername/distributed-kv/internal/raft"
	"github.com/yourusername/distributed-kv/internal/storage"
	"github.com/yourusername/distributed-kv/internal/transport"
)

// TestCluster manages a cluster of RaftNodes for testing.
type TestCluster struct {
	mu           sync.RWMutex
	nodes        map[string]*TestNode
	nodeOrder    []string          // Order of nodes for deterministic iteration
	stoppedNodes map[string]bool   // Tracks which nodes have been stopped
	started      bool
}

// TestNode wraps a RaftNode with its infrastructure for testing.
type TestNode struct {
	ID        string
	Node      *raft.RaftNode
	Storage   storage.Storage
	Transport *transport.GRPCTransport
	Addr      string
	ApplyCh   chan raft.ApplyMsg
}

// NewTestCluster creates a new test cluster with n nodes.
func NewTestCluster(n int) *TestCluster {
	cluster := &TestCluster{
		nodes:        make(map[string]*TestNode),
		nodeOrder:    make([]string, 0, n),
		stoppedNodes: make(map[string]bool),
	}

	// Generate node IDs and addresses
	nodeIDs := make([]string, n)
	nodeAddrs := make(map[string]string)

	for i := 0; i < n; i++ {
		nodeIDs[i] = fmt.Sprintf("node%d", i+1)
		addr := getFreeAddr()
		nodeAddrs[nodeIDs[i]] = addr
	}

	// Create nodes
	for i := 0; i < n; i++ {
		nodeID := nodeIDs[i]
		addr := nodeAddrs[nodeID]

		// Get peers (all nodes except self)
		peers := make([]string, 0, n-1)
		for _, id := range nodeIDs {
			if id != nodeID {
				peers = append(peers, id)
			}
		}

		node, err := createTestNode(nodeID, addr, peers, nodeAddrs)
		if err != nil {
			panic(fmt.Sprintf("failed to create node %s: %v", nodeID, err))
		}

		cluster.nodes[nodeID] = node
		cluster.nodeOrder = append(cluster.nodeOrder, nodeID)
	}

	return cluster
}

// createTestNode creates a single test node.
func createTestNode(id, addr string, peers []string, peerAddrs map[string]string) (*TestNode, error) {
	// Create in-memory storage
	store := storage.NewMemoryStorage()

	// Create transport
	transportCfg := transport.DefaultGRPCTransportConfig(addr)
	trans := transport.NewGRPCTransport(transportCfg)

	// Create apply channel
	applyCh := make(chan raft.ApplyMsg, 100)

	// Create RaftNode
	cfg := &raft.RaftConfig{
		ID:                 id,
		Peers:              peers,
		PeerAddrs:          peerAddrs,
		Storage:            store,
		Transport:          trans,
		ApplyCh:            applyCh,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}

	node, err := raft.NewRaftNode(cfg)
	if err != nil {
		return nil, err
	}

	return &TestNode{
		ID:        id,
		Node:      node,
		Storage:   store,
		Transport: trans,
		Addr:      addr,
		ApplyCh:   applyCh,
	}, nil
}

// getFreeAddr returns a free local address for testing.
func getFreeAddr() string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// Start starts all nodes in the cluster.
func (c *TestCluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return nil
	}

	// Check if this is a restart (nodes already exist but were stopped)
	// We need to recreate RaftNodes since channels cannot be reopened
	for _, id := range c.nodeOrder {
		oldNode := c.nodes[id]

		// Check if we need to recreate the node (transport was stopped)
		// We'll recreate both transport and raft node to ensure clean state

		// Get peers
		peers := make([]string, 0, len(c.nodes)-1)
		peerAddrs := make(map[string]string)
		for nodeID, node := range c.nodes {
			peerAddrs[nodeID] = node.Addr
			if nodeID != id {
				peers = append(peers, nodeID)
			}
		}

		// Create new transport with same address
		transportCfg := transport.DefaultGRPCTransportConfig(oldNode.Addr)
		trans := transport.NewGRPCTransport(transportCfg)

		// Create new RaftNode with same storage (for persistence) and same apply channel
		cfg := &raft.RaftConfig{
			ID:                 id,
			Peers:              peers,
			PeerAddrs:          peerAddrs,
			Storage:            oldNode.Storage,
			Transport:          trans,
			ApplyCh:            oldNode.ApplyCh,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		}

		node, err := raft.NewRaftNode(cfg)
		if err != nil {
			return fmt.Errorf("failed to create raft node %s: %w", id, err)
		}

		c.nodes[id] = &TestNode{
			ID:        id,
			Node:      node,
			Storage:   oldNode.Storage,
			Transport: trans,
			Addr:      oldNode.Addr,
			ApplyCh:   oldNode.ApplyCh,
		}
	}

	// Start transports first
	for _, node := range c.nodes {
		if err := node.Transport.Start(); err != nil {
			return fmt.Errorf("failed to start transport for %s: %w", node.ID, err)
		}
	}

	// Small delay to ensure transports are ready
	time.Sleep(50 * time.Millisecond)

	// Start Raft nodes
	for _, node := range c.nodes {
		if err := node.Node.Start(); err != nil {
			return fmt.Errorf("failed to start raft node %s: %w", node.ID, err)
		}
	}

	// Clear all stopped node flags since all nodes are now running
	c.stoppedNodes = make(map[string]bool)

	c.started = true
	return nil
}

// Stop stops all nodes in the cluster.
// Note: This does NOT close storage, so the cluster can be restarted.
// Use Cleanup() to fully clean up resources.
func (c *TestCluster) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return nil
	}

	// Stop Raft nodes first
	for _, node := range c.nodes {
		node.Node.Stop()
	}

	// Stop transports
	for _, node := range c.nodes {
		node.Transport.Stop()
	}

	// Note: We don't close storage here to allow restart with persistence
	// Use Cleanup() when done with the cluster entirely

	c.started = false
	return nil
}

// Cleanup fully cleans up all cluster resources including storage.
// Call this instead of Stop() when you're completely done with the cluster.
func (c *TestCluster) Cleanup() error {
	c.Stop()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Close storages
	for _, node := range c.nodes {
		node.Storage.Close()
	}

	return nil
}

// Leader returns the current leader, or nil if no leader.
// Excludes stopped nodes from consideration.
func (c *TestCluster) Leader() *TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, id := range c.nodeOrder {
		// Skip stopped nodes
		if c.stoppedNodes[id] {
			continue
		}
		node := c.nodes[id]
		if node.Node.State() == raft.Leader {
			return node
		}
	}
	return nil
}

// WaitForLeader waits for a leader to be elected within the timeout.
func (c *TestCluster) WaitForLeader(timeout time.Duration) *TestNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader := c.Leader(); leader != nil {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// GetNode returns the node with the given ID.
func (c *TestCluster) GetNode(id string) *TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

// Nodes returns all nodes in the cluster.
func (c *TestCluster) Nodes() []*TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*TestNode, 0, len(c.nodes))
	for _, id := range c.nodeOrder {
		nodes = append(nodes, c.nodes[id])
	}
	return nodes
}

// Followers returns all follower nodes.
func (c *TestCluster) Followers() []*TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	followers := make([]*TestNode, 0)
	for _, id := range c.nodeOrder {
		node := c.nodes[id]
		if node.Node.State() == raft.Follower {
			followers = append(followers, node)
		}
	}
	return followers
}

// StopNode stops a specific node (simulates failure).
func (c *TestCluster) StopNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}

	node.Node.Stop()
	node.Transport.Stop()
	c.stoppedNodes[id] = true
	return nil
}

// RestartNode restarts a stopped node.
func (c *TestCluster) RestartNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	oldNode, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}

	// Get peers
	peers := make([]string, 0, len(c.nodes)-1)
	peerAddrs := make(map[string]string)
	for nodeID, node := range c.nodes {
		peerAddrs[nodeID] = node.Addr
		if nodeID != id {
			peers = append(peers, nodeID)
		}
	}

	// Create new transport with same address
	transportCfg := transport.DefaultGRPCTransportConfig(oldNode.Addr)
	trans := transport.NewGRPCTransport(transportCfg)

	// Create new RaftNode with same storage (for persistence)
	cfg := &raft.RaftConfig{
		ID:                 id,
		Peers:              peers,
		PeerAddrs:          peerAddrs,
		Storage:            oldNode.Storage,
		Transport:          trans,
		ApplyCh:            oldNode.ApplyCh,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}

	node, err := raft.NewRaftNode(cfg)
	if err != nil {
		return err
	}

	// Start transport
	if err := trans.Start(); err != nil {
		return err
	}

	time.Sleep(20 * time.Millisecond)

	// Start node
	if err := node.Start(); err != nil {
		return err
	}

	c.nodes[id] = &TestNode{
		ID:        id,
		Node:      node,
		Storage:   oldNode.Storage,
		Transport: trans,
		Addr:      oldNode.Addr,
		ApplyCh:   oldNode.ApplyCh,
	}

	// Clear the stopped flag
	delete(c.stoppedNodes, id)

	return nil
}

// WaitForApply waits for an entry with the given index to be applied on the node.
func WaitForApply(applyCh <-chan raft.ApplyMsg, index int64, timeout time.Duration) (raft.ApplyMsg, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case msg := <-applyCh:
			if msg.CommandIndex == index {
				return msg, true
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	return raft.ApplyMsg{}, false
}

// WaitForCondition waits for a condition to be true within timeout.
func WaitForCondition(fn func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// AllNodesHaveEntry returns true if all nodes have the given entry.
func (c *TestCluster) AllNodesHaveEntry(index int64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, id := range c.nodeOrder {
		node := c.nodes[id]
		if node.Node.CommitIndex() < index {
			return false
		}
	}
	return true
}

// DrainApplyChannels drains all apply channels without blocking.
func (c *TestCluster) DrainApplyChannels() {
	for _, node := range c.Nodes() {
		for {
			select {
			case <-node.ApplyCh:
			default:
				goto next
			}
		}
	next:
	}
}
