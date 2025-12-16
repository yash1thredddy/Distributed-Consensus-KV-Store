package server_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/server"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/pkg/testutil"
)

// TestKVCluster wraps a test cluster with KV servers.
type TestKVCluster struct {
	cluster   *testutil.TestCluster
	kvServers map[string]*server.KVServer
}

// NewTestKVCluster creates a test cluster with KV servers.
func NewTestKVCluster(n int) *TestKVCluster {
	cluster := testutil.NewTestCluster(n)
	return &TestKVCluster{
		cluster:   cluster,
		kvServers: make(map[string]*server.KVServer),
	}
}

// Start starts the cluster and KV servers.
func (c *TestKVCluster) Start() error {
	if err := c.cluster.Start(); err != nil {
		return err
	}

	// Create KV servers for each node
	for _, node := range c.cluster.Nodes() {
		cfg := &server.KVServerConfig{
			Raft:             node.Node,
			Storage:          node.Storage,
			OperationTimeout: 5 * time.Second,
		}
		kv := server.NewKVServer(cfg)
		if err := kv.Start(); err != nil {
			// Clean up any KV servers started before the error
			for _, startedKV := range c.kvServers {
				startedKV.Stop()
			}
			return err
		}
		c.kvServers[node.ID] = kv
	}

	return nil
}

// Cleanup stops everything and cleans up resources.
func (c *TestKVCluster) Cleanup() {
	// Stop KV servers first
	for _, kv := range c.kvServers {
		kv.Stop()
	}
	c.cluster.Cleanup()
}

// WaitForLeader waits for a leader to be elected.
func (c *TestKVCluster) WaitForLeader(timeout time.Duration) *server.KVServer {
	leader := c.cluster.WaitForLeader(timeout)
	if leader == nil {
		return nil
	}
	return c.kvServers[leader.ID]
}

// GetLeaderKV returns the KV server for the current leader.
func (c *TestKVCluster) GetLeaderKV() *server.KVServer {
	leader := c.cluster.Leader()
	if leader == nil {
		return nil
	}
	return c.kvServers[leader.ID]
}

// GetFollowerKV returns a KV server for a follower node.
func (c *TestKVCluster) GetFollowerKV() *server.KVServer {
	followers := c.cluster.Followers()
	if len(followers) == 0 {
		return nil
	}
	return c.kvServers[followers[0].ID]
}

// AllKVServers returns all KV servers.
func (c *TestKVCluster) AllKVServers() []*server.KVServer {
	servers := make([]*server.KVServer, 0, len(c.kvServers))
	for _, kv := range c.kvServers {
		servers = append(servers, kv)
	}
	return servers
}

func TestKVServer_BasicPutGet(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "expected a leader to be elected")

	ctx := context.Background()

	// Put a key
	err := leader.Put(ctx, "key1", []byte("value1"))
	require.NoError(t, err)

	// Get the key
	value, found, err := leader.Get(ctx, "key1", false)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), value)
}

func TestKVServer_GetNonExistent(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	ctx := context.Background()

	// Get non-existent key
	value, found, err := leader.Get(ctx, "nonexistent", false)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestKVServer_PutOverwrite(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	ctx := context.Background()

	// Put initial value
	err := leader.Put(ctx, "key1", []byte("value1"))
	require.NoError(t, err)

	// Overwrite
	err = leader.Put(ctx, "key1", []byte("value2"))
	require.NoError(t, err)

	// Get should return new value
	value, found, err := leader.Get(ctx, "key1", false)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value2"), value)
}

func TestKVServer_Delete(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	ctx := context.Background()

	// Put a key
	err := leader.Put(ctx, "key1", []byte("value1"))
	require.NoError(t, err)

	// Delete
	err = leader.Delete(ctx, "key1")
	require.NoError(t, err)

	// Get should not find it
	value, found, err := leader.Get(ctx, "key1", false)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestKVServer_LinearizableRead(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	ctx := context.Background()

	// Put a value
	err := leader.Put(ctx, "key1", []byte("value1"))
	require.NoError(t, err)

	// Linearizable read on leader
	value, found, err := leader.Get(ctx, "key1", true)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), value)
}

func TestKVServer_LeaderRedirect(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Wait for heartbeats to propagate so followers know about the leader
	// Use a retry loop instead of fixed sleep for reliability on slow CI systems
	var follower *server.KVServer
	require.Eventually(t, func() bool {
		follower = cluster.GetFollowerKV()
		if follower == nil {
			return false
		}
		// Check if follower knows about the leader by attempting an operation
		err := follower.Put(context.Background(), "test-key", []byte("test"))
		if err == nil {
			return false // Follower thinks it's the leader, not ready yet
		}
		var notLeaderErr *server.ErrNotLeaderWithHint
		if errors.As(err, &notLeaderErr) {
			return notLeaderErr.LeaderID != "" // Follower knows who the leader is
		}
		return false
	}, 2*time.Second, 50*time.Millisecond, "follower should become aware of leader")

	require.NotNil(t, follower)

	ctx := context.Background()

	// Put on follower should fail with leader hint
	err := follower.Put(ctx, "key1", []byte("value1"))
	require.Error(t, err)

	// Should be ErrNotLeaderWithHint
	var notLeaderErr *server.ErrNotLeaderWithHint
	require.ErrorAs(t, err, &notLeaderErr)
	assert.NotEmpty(t, notLeaderErr.LeaderID)
}

func TestKVServer_ConcurrentOperations(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	ctx := context.Background()
	numOps := 50
	var wg sync.WaitGroup

	// Concurrent puts
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			value := []byte(fmt.Sprintf("value%d", i))
			if err := leader.Put(ctx, key, value); err != nil {
				t.Logf("Put failed for key %s: %v", key, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify we can still read
	_, _, err := leader.Get(ctx, "key0", false)
	require.NoError(t, err)
}

func TestKVServer_MultipleKeys(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	ctx := context.Background()

	// Put multiple keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range keys {
		err := leader.Put(ctx, key, []byte("value-"+key))
		require.NoError(t, err)
	}

	// Verify all keys
	for _, key := range keys {
		value, found, err := leader.Get(ctx, key, false)
		require.NoError(t, err)
		assert.True(t, found, "key %s should exist", key)
		assert.Equal(t, []byte("value-"+key), value)
	}
}

func TestKVServer_LinearizableReadOnFollower(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Get a follower
	follower := cluster.GetFollowerKV()
	require.NotNil(t, follower)

	ctx := context.Background()

	// Linearizable read on follower should fail (need to go through leader)
	_, _, err := follower.Get(ctx, "key1", true)
	require.Error(t, err)

	var notLeaderErr *server.ErrNotLeaderWithHint
	require.ErrorAs(t, err, &notLeaderErr)
}

func TestKVServer_ContextCancellation(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Put with cancelled context should fail
	err := leader.Put(ctx, "key1", []byte("value1"))
	require.Error(t, err)
}

func TestKVServer_DeleteNonExistent(t *testing.T) {
	cluster := NewTestKVCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	ctx := context.Background()

	// Delete non-existent key should not error
	err := leader.Delete(ctx, "nonexistent")
	require.NoError(t, err)
}
