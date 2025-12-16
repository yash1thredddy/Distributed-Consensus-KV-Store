package testutil

import (
	"fmt"
	"sync"
	"time"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/raft"
)

// ClusterConfig holds configuration for creating a test cluster.
type ClusterConfig struct {
	NumNodes           int
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

// DefaultClusterConfig returns default cluster configuration.
func DefaultClusterConfig(numNodes int) ClusterConfig {
	return ClusterConfig{
		NumNodes:           numNodes,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
}

// TestCluster manages a cluster of RaftNodes for testing.
type TestCluster struct {
	mu           sync.RWMutex
	nodes        map[string]*TestNode
	nodeOrder    []string          // Order of nodes for deterministic iteration
	stoppedNodes map[string]bool   // Tracks which nodes have been stopped
	started      bool              // Whether the cluster is currently running
	initialized  bool              // Whether the cluster has been started at least once
	config       ClusterConfig
}

// NewTestCluster creates a new test cluster with n nodes.
func NewTestCluster(n int) *TestCluster {
	return NewTestClusterWithConfig(DefaultClusterConfig(n))
}

// NewTestClusterWithConfig creates a new test cluster with custom configuration.
func NewTestClusterWithConfig(cfg ClusterConfig) *TestCluster {
	cluster := &TestCluster{
		nodes:        make(map[string]*TestNode),
		nodeOrder:    make([]string, 0, cfg.NumNodes),
		stoppedNodes: make(map[string]bool),
		config:       cfg,
	}

	// Generate node IDs and addresses
	nodeIDs := make([]string, cfg.NumNodes)
	nodeAddrs := make(map[string]string)

	for i := 0; i < cfg.NumNodes; i++ {
		nodeIDs[i] = fmt.Sprintf("node%d", i+1)
		nodeAddrs[nodeIDs[i]] = GetFreeAddr()
	}

	// Create nodes
	for i := 0; i < cfg.NumNodes; i++ {
		nodeID := nodeIDs[i]
		addr := nodeAddrs[nodeID]

		// Get peers (all nodes except self)
		peers := make([]string, 0, cfg.NumNodes-1)
		for _, id := range nodeIDs {
			if id != nodeID {
				peers = append(peers, id)
			}
		}

		nodeCfg := TestNodeConfig{
			ID:                 nodeID,
			Addr:               addr,
			Peers:              peers,
			PeerAddrs:          nodeAddrs,
			ElectionTimeoutMin: cfg.ElectionTimeoutMin,
			ElectionTimeoutMax: cfg.ElectionTimeoutMax,
			HeartbeatInterval:  cfg.HeartbeatInterval,
		}

		node, err := NewTestNode(nodeCfg)
		if err != nil {
			panic(fmt.Sprintf("failed to create node %s: %v", nodeID, err))
		}

		cluster.nodes[nodeID] = node
		cluster.nodeOrder = append(cluster.nodeOrder, nodeID)
	}

	return cluster
}

// Start starts all nodes in the cluster.
func (c *TestCluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return nil
	}

	// Only recreate nodes on restart (not on first start).
	// Channels cannot be reopened, so we need fresh nodes for restarts.
	if c.initialized {
		if err := c.recreateNodes(); err != nil {
			return err
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

	// Clear all stopped node flags
	c.stoppedNodes = make(map[string]bool)
	c.started = true
	c.initialized = true
	return nil
}

// recreateNodes recreates RaftNode instances (needed for restart).
func (c *TestCluster) recreateNodes() error {
	for _, id := range c.nodeOrder {
		oldNode := c.nodes[id]

		// Build peer list
		peers := make([]string, 0, len(c.nodes)-1)
		peerAddrs := make(map[string]string)
		for nodeID, node := range c.nodes {
			peerAddrs[nodeID] = node.Addr
			if nodeID != id {
				peers = append(peers, nodeID)
			}
		}

		nodeCfg := TestNodeConfig{
			ID:                 id,
			Addr:               oldNode.Addr,
			Peers:              peers,
			PeerAddrs:          peerAddrs,
			Storage:            oldNode.Storage, // Preserve storage for persistence
			ApplyCh:            oldNode.ApplyCh, // Preserve apply channel
			ElectionTimeoutMin: c.config.ElectionTimeoutMin,
			ElectionTimeoutMax: c.config.ElectionTimeoutMax,
			HeartbeatInterval:  c.config.HeartbeatInterval,
		}

		node, err := NewTestNode(nodeCfg)
		if err != nil {
			return fmt.Errorf("failed to create raft node %s: %w", id, err)
		}

		c.nodes[id] = node
	}
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

	for _, node := range c.nodes {
		node.Stop()
	}

	c.started = false
	return nil
}

// Cleanup fully cleans up all cluster resources including storage.
func (c *TestCluster) Cleanup() error {
	c.Stop()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, node := range c.nodes {
		node.Storage.Close()
	}

	return nil
}

// Leader returns the current leader, or nil if no leader.
func (c *TestCluster) Leader() *TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, id := range c.nodeOrder {
		if c.stoppedNodes[id] {
			continue
		}
		if node := c.nodes[id]; node.IsLeader() {
			return node
		}
	}
	return nil
}

// WaitForLeader waits for a leader to be elected within the timeout.
func (c *TestCluster) WaitForLeader(timeout time.Duration) *TestNode {
	var leader *TestNode
	WaitForCondition(func() bool {
		leader = c.Leader()
		return leader != nil
	}, timeout)
	return leader
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

// Followers returns all follower nodes (excludes stopped nodes).
func (c *TestCluster) Followers() []*TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	followers := make([]*TestNode, 0)
	for _, id := range c.nodeOrder {
		// Skip stopped nodes for consistency with Leader()
		if c.stoppedNodes[id] {
			continue
		}
		if node := c.nodes[id]; node.IsFollower() {
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

	node.Stop()
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

	// Build peer list
	peers := make([]string, 0, len(c.nodes)-1)
	peerAddrs := make(map[string]string)
	for nodeID, node := range c.nodes {
		peerAddrs[nodeID] = node.Addr
		if nodeID != id {
			peers = append(peers, nodeID)
		}
	}

	nodeCfg := TestNodeConfig{
		ID:                 id,
		Addr:               oldNode.Addr,
		Peers:              peers,
		PeerAddrs:          peerAddrs,
		Storage:            oldNode.Storage,
		ApplyCh:            oldNode.ApplyCh,
		ElectionTimeoutMin: c.config.ElectionTimeoutMin,
		ElectionTimeoutMax: c.config.ElectionTimeoutMax,
		HeartbeatInterval:  c.config.HeartbeatInterval,
	}

	node, err := NewTestNode(nodeCfg)
	if err != nil {
		return err
	}

	if err := node.Start(); err != nil {
		return err
	}

	c.nodes[id] = node
	delete(c.stoppedNodes, id)
	return nil
}

// AllNodesHaveEntry returns true if all running nodes have committed the given entry.
// Stopped nodes are excluded to prevent WaitForCommit from hanging.
func (c *TestCluster) AllNodesHaveEntry(index int64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, id := range c.nodeOrder {
		// Skip stopped nodes - they won't have current commit index
		if c.stoppedNodes[id] {
			continue
		}
		if c.nodes[id].CommitIndex() < index {
			return false
		}
	}
	return true
}

// DrainApplyChannels drains all apply channels without blocking.
func (c *TestCluster) DrainApplyChannels() {
	for _, node := range c.Nodes() {
		DrainChannel(node.ApplyCh)
	}
}

// WaitForCommit waits for all nodes to commit the given index.
func (c *TestCluster) WaitForCommit(index int64, timeout time.Duration) bool {
	return WaitForCondition(func() bool {
		return c.AllNodesHaveEntry(index)
	}, timeout)
}

// NodeCount returns the number of nodes in the cluster.
func (c *TestCluster) NodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.nodes)
}

// RunningNodeCount returns the number of running (non-stopped) nodes.
func (c *TestCluster) RunningNodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.nodes) - len(c.stoppedNodes)
}

// ProposeOnLeader proposes a command on the current leader.
// Returns the log index and term, or error if no leader or proposal fails.
func (c *TestCluster) ProposeOnLeader(cmd *raft.Command) (int64, int64, error) {
	leader := c.Leader()
	if leader == nil {
		return 0, 0, fmt.Errorf("no leader available")
	}

	data, err := cmd.Encode()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to encode command: %w", err)
	}

	return leader.Node.Propose(data)
}
