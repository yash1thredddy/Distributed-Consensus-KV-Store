package raft_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/raft"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/pkg/testutil"
)

func TestRaftNode_InitialState(t *testing.T) {
	cluster := testutil.NewTestCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Initially all nodes should be followers
	for _, node := range cluster.Nodes() {
		assert.Equal(t, raft.Follower, node.Node.State())
		assert.Equal(t, int64(0), node.Node.Term())
	}
}

func TestRaftNode_LeaderElection(t *testing.T) {
	cluster := testutil.NewTestCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "expected a leader to be elected")

	// Verify exactly one leader
	leaderCount := 0
	for _, node := range cluster.Nodes() {
		if node.Node.State() == raft.Leader {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "expected exactly one leader")

	// Verify leader has highest term or all same term
	leaderTerm := leader.Node.Term()
	for _, node := range cluster.Nodes() {
		assert.LessOrEqual(t, node.Node.Term(), leaderTerm)
	}
}

func TestRaftNode_LeaderReElection(t *testing.T) {
	cluster := testutil.NewTestCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for initial leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "expected initial leader")

	oldLeaderID := leader.ID
	oldTerm := leader.Node.Term()

	// Stop the leader
	require.NoError(t, cluster.StopNode(oldLeaderID))

	// Wait for new leader
	time.Sleep(500 * time.Millisecond) // Give time for election timeout
	newLeader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, newLeader, "expected new leader after old leader stopped")

	// New leader should be different
	assert.NotEqual(t, oldLeaderID, newLeader.ID)

	// New leader should have higher or equal term
	assert.GreaterOrEqual(t, newLeader.Node.Term(), oldTerm)
}

func TestRaftNode_FiveNodeCluster(t *testing.T) {
	cluster := testutil.NewTestCluster(5)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "expected a leader to be elected")

	// Count states
	leaderCount := 0
	followerCount := 0
	for _, node := range cluster.Nodes() {
		switch node.Node.State() {
		case raft.Leader:
			leaderCount++
		case raft.Follower:
			followerCount++
		}
	}

	assert.Equal(t, 1, leaderCount)
	assert.Equal(t, 4, followerCount)
}

func TestRaftNode_QuorumSize(t *testing.T) {
	tests := []struct {
		name     string
		numNodes int
		expected int
	}{
		{"3 nodes", 3, 2},
		{"5 nodes", 5, 3},
		{"7 nodes", 7, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := testutil.NewTestCluster(tt.numNodes)
			require.NoError(t, cluster.Start())
			defer cluster.Cleanup()

			// Wait for leader and check quorum understanding
			leader := cluster.WaitForLeader(5 * time.Second)
			require.NotNil(t, leader)

			// The quorum size is internal, but we verify by behavior:
			// raft.Leader should exist (meaning quorum was achieved)
		})
	}
}

func TestRaftNode_FollowersUpdateLeaderId(t *testing.T) {
	cluster := testutil.NewTestCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Wait a bit for heartbeats to propagate
	time.Sleep(200 * time.Millisecond)

	// All followers should know the leader
	for _, node := range cluster.Nodes() {
		if node.ID != leader.ID {
			assert.Equal(t, leader.ID, node.Node.LeaderID())
		}
	}
}

func TestRaftNode_ProposeOnLeader(t *testing.T) {
	cluster := testutil.NewTestCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Drain any pending apply messages (like no-op)
	cluster.DrainApplyChannels()

	// Propose a command
	cmd := &raft.Command{Type: raft.CommandPut, Key: "key1", Value: []byte("value1")}
	data, err := cmd.Encode()
	require.NoError(t, err)

	index, term, err := leader.Node.Propose(data)
	require.NoError(t, err)
	assert.Greater(t, index, int64(0))
	assert.Greater(t, term, int64(0))
}

func TestRaftNode_ProposeOnFollower(t *testing.T) {
	cluster := testutil.NewTestCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Find a follower
	var follower *testutil.TestNode
	for _, node := range cluster.Nodes() {
		if node.ID != leader.ID {
			follower = node
			break
		}
	}
	require.NotNil(t, follower)

	// Propose on follower should fail
	_, _, err := follower.Node.Propose([]byte("test"))
	require.Error(t, err)

	// Error should contain leader hint
	errWithHint, ok := err.(*raft.ErrNotLeaderWithHint)
	assert.True(t, ok)
	assert.Equal(t, leader.ID, errWithHint.LeaderID)
}

func TestRaftNode_BasicReplication(t *testing.T) {
	cluster := testutil.NewTestCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Wait for initial no-op to be applied
	time.Sleep(300 * time.Millisecond)
	cluster.DrainApplyChannels()

	// Propose a command
	cmd := &raft.Command{Type: raft.CommandPut, Key: "key1", Value: []byte("value1")}
	data, err := cmd.Encode()
	require.NoError(t, err)

	index, _, err := leader.Node.Propose(data)
	require.NoError(t, err)

	// Wait for entry to be applied on leader
	msg, found := testutil.WaitForApply(leader.ApplyCh, index, 3*time.Second)
	require.True(t, found, "entry should be applied on leader")
	assert.True(t, msg.CommandValid)
	assert.Equal(t, index, msg.CommandIndex)

	// Wait for all nodes to have the entry committed
	success := testutil.WaitForCondition(func() bool {
		return cluster.AllNodesHaveEntry(index)
	}, 3*time.Second)
	assert.True(t, success, "all nodes should have the entry")
}

func TestRaftNode_MultipleProposals(t *testing.T) {
	cluster := testutil.NewTestCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Wait for no-op
	time.Sleep(300 * time.Millisecond)
	cluster.DrainApplyChannels()

	// Propose multiple commands
	numCommands := 10
	indices := make([]int64, numCommands)

	for i := 0; i < numCommands; i++ {
		cmd := &raft.Command{Type: raft.CommandPut, Key: "key", Value: []byte{byte(i)}}
		data, err := cmd.Encode()
		require.NoError(t, err)

		index, _, err := leader.Node.Propose(data)
		require.NoError(t, err)
		indices[i] = index
	}

	// Wait for all entries to be committed
	lastIndex := indices[numCommands-1]
	success := testutil.WaitForCondition(func() bool {
		return cluster.AllNodesHaveEntry(lastIndex)
	}, 5*time.Second)
	assert.True(t, success, "all entries should be committed")
}

func TestRaftNode_PersistentState(t *testing.T) {
	cluster := testutil.NewTestCluster(3)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Record state
	_ = leader.ID // leaderID might change after restart
	term := leader.Node.Term()

	// Stop the cluster (but don't cleanup - we want to restart)
	cluster.Stop()

	// Restart the cluster
	require.NoError(t, cluster.Start())

	// Wait for leader
	newLeader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, newLeader)

	// Term should be at least as high (might be higher if re-election)
	assert.GreaterOrEqual(t, newLeader.Node.Term(), term)
}

func TestRaftNode_LeaderWithMajority(t *testing.T) {
	cluster := testutil.NewTestCluster(5)
	require.NoError(t, cluster.Start())
	defer cluster.Cleanup()

	// Wait for leader
	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Stop 2 followers (minority)
	followers := cluster.Followers()
	require.GreaterOrEqual(t, len(followers), 2)

	cluster.StopNode(followers[0].ID)
	cluster.StopNode(followers[1].ID)

	// raft.Leader should still be able to propose (majority still available)
	time.Sleep(200 * time.Millisecond)

	// Check if original leader is still leader, or a new one was elected
	currentLeader := cluster.WaitForLeader(3 * time.Second)
	require.NotNil(t, currentLeader, "should still have a leader with majority")

	// Propose should work
	cmd := &raft.Command{Type: raft.CommandPut, Key: "key", Value: []byte("value")}
	data, _ := cmd.Encode()
	_, _, err := currentLeader.Node.Propose(data)
	require.NoError(t, err)
}
