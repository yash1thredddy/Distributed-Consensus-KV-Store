package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yourusername/distributed-kv/api/proto/raftpb"
)

// mockRaftHandler implements RaftHandler for testing.
type mockRaftHandler struct {
	requestVoteHandler     func(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	appendEntriesHandler   func(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	installSnapshotHandler func(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

func (h *mockRaftHandler) HandleRequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	if h.requestVoteHandler != nil {
		return h.requestVoteHandler(ctx, req)
	}
	return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (h *mockRaftHandler) HandleAppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	if h.appendEntriesHandler != nil {
		return h.appendEntriesHandler(ctx, req)
	}
	return &raftpb.AppendEntriesResponse{Term: req.Term, Success: true, MatchIndex: req.PrevLogIndex + int64(len(req.Entries))}, nil
}

func (h *mockRaftHandler) HandleInstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	if h.installSnapshotHandler != nil {
		return h.installSnapshotHandler(ctx, req)
	}
	return &raftpb.InstallSnapshotResponse{Term: req.Term}, nil
}

// getFreePort returns a free port for testing.
func getFreePort(t *testing.T) string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// createTestTransport creates a transport for testing.
func createTestTransport(t *testing.T) (*GRPCTransport, string) {
	addr := getFreePort(t)
	cfg := DefaultGRPCTransportConfig(addr)
	transport := NewGRPCTransport(cfg)
	return transport, addr
}

func TestGRPCTransport_StartStop(t *testing.T) {
	transport, _ := createTestTransport(t)

	// Start should succeed
	err := transport.Start()
	require.NoError(t, err)

	// Starting again should fail
	err = transport.Start()
	require.Error(t, err)

	// Stop should succeed
	err = transport.Stop()
	require.NoError(t, err)

	// Stop again should be idempotent
	err = transport.Stop()
	require.NoError(t, err)
}

func TestGRPCTransport_BasicConnectivity(t *testing.T) {
	// Create two transports
	transport1, addr1 := createTestTransport(t)
	transport2, _ := createTestTransport(t)

	// Register handlers
	var receivedReq *raftpb.RequestVoteRequest
	handler1 := &mockRaftHandler{
		requestVoteHandler: func(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
			receivedReq = req
			return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
	}
	transport1.RegisterRaftHandler(handler1)
	transport2.RegisterRaftHandler(&mockRaftHandler{})

	// Start both transports
	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	defer transport1.Stop()
	defer transport2.Stop()

	// Give servers time to start
	time.Sleep(50 * time.Millisecond)

	// Send RequestVote from transport2 to transport1
	req := &raftpb.RequestVoteRequest{
		Term:         5,
		CandidateId:  "node2",
		LastLogIndex: 10,
		LastLogTerm:  4,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := transport2.SendRequestVote(ctx, addr1, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify response
	assert.Equal(t, int64(5), resp.Term)
	assert.True(t, resp.VoteGranted)

	// Verify request was received correctly
	require.NotNil(t, receivedReq)
	assert.Equal(t, int64(5), receivedReq.Term)
	assert.Equal(t, "node2", receivedReq.CandidateId)
	assert.Equal(t, int64(10), receivedReq.LastLogIndex)
	assert.Equal(t, int64(4), receivedReq.LastLogTerm)
}

func TestGRPCTransport_AppendEntries(t *testing.T) {
	transport1, addr1 := createTestTransport(t)
	transport2, _ := createTestTransport(t)

	var receivedReq *raftpb.AppendEntriesRequest
	handler1 := &mockRaftHandler{
		appendEntriesHandler: func(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
			receivedReq = req
			return &raftpb.AppendEntriesResponse{
				Term:       req.Term,
				Success:    true,
				MatchIndex: req.PrevLogIndex + int64(len(req.Entries)),
			}, nil
		},
	}
	transport1.RegisterRaftHandler(handler1)
	transport2.RegisterRaftHandler(&mockRaftHandler{})

	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	defer transport1.Stop()
	defer transport2.Stop()

	time.Sleep(50 * time.Millisecond)

	// Send AppendEntries with multiple entries
	req := &raftpb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "leader1",
		PrevLogIndex: 5,
		PrevLogTerm:  2,
		Entries: []*raftpb.LogEntry{
			{Term: 3, Index: 6, Command: []byte("cmd1")},
			{Term: 3, Index: 7, Command: []byte("cmd2")},
		},
		LeaderCommit: 4,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := transport2.SendAppendEntries(ctx, addr1, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, int64(3), resp.Term)
	assert.True(t, resp.Success)
	assert.Equal(t, int64(7), resp.MatchIndex)

	require.NotNil(t, receivedReq)
	assert.Equal(t, "leader1", receivedReq.LeaderId)
	assert.Len(t, receivedReq.Entries, 2)
}

func TestGRPCTransport_InstallSnapshot(t *testing.T) {
	transport1, addr1 := createTestTransport(t)
	transport2, _ := createTestTransport(t)

	var receivedReq *raftpb.InstallSnapshotRequest
	handler1 := &mockRaftHandler{
		installSnapshotHandler: func(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
			receivedReq = req
			return &raftpb.InstallSnapshotResponse{Term: req.Term}, nil
		},
	}
	transport1.RegisterRaftHandler(handler1)
	transport2.RegisterRaftHandler(&mockRaftHandler{})

	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	defer transport1.Stop()
	defer transport2.Stop()

	time.Sleep(50 * time.Millisecond)

	// Create snapshot data
	snapshotData := make([]byte, 1024)
	for i := range snapshotData {
		snapshotData[i] = byte(i % 256)
	}

	req := &raftpb.InstallSnapshotRequest{
		Term:              5,
		LeaderId:          "leader1",
		LastIncludedIndex: 100,
		LastIncludedTerm:  4,
		Data:              snapshotData,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := transport2.SendInstallSnapshot(ctx, addr1, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, int64(5), resp.Term)

	require.NotNil(t, receivedReq)
	assert.Equal(t, int64(100), receivedReq.LastIncludedIndex)
	assert.Equal(t, snapshotData, receivedReq.Data)
}

func TestGRPCTransport_ConnectionPooling(t *testing.T) {
	transport1, addr1 := createTestTransport(t)
	transport2, _ := createTestTransport(t)

	transport1.RegisterRaftHandler(&mockRaftHandler{})
	transport2.RegisterRaftHandler(&mockRaftHandler{})

	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	defer transport1.Stop()
	defer transport2.Stop()

	time.Sleep(50 * time.Millisecond)

	// Send multiple RPCs to the same peer
	for i := 0; i < 10; i++ {
		req := &raftpb.RequestVoteRequest{
			Term:        int64(i),
			CandidateId: fmt.Sprintf("node%d", i),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := transport2.SendRequestVote(ctx, addr1, req)
		cancel()
		require.NoError(t, err)
	}

	// Verify only one connection was created
	connCount := transport2.GetConnectionCount()
	assert.Equal(t, 1, connCount, "should reuse single connection")
}

func TestGRPCTransport_TimeoutHandling(t *testing.T) {
	transport1, addr1 := createTestTransport(t)
	transport2, _ := createTestTransport(t)

	// Create handler that sleeps longer than timeout
	handler1 := &mockRaftHandler{
		requestVoteHandler: func(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
			select {
			case <-time.After(5 * time.Second):
				return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	transport1.RegisterRaftHandler(handler1)
	transport2.RegisterRaftHandler(&mockRaftHandler{})

	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	defer transport1.Stop()
	defer transport2.Stop()

	time.Sleep(50 * time.Millisecond)

	// Send request with short timeout
	req := &raftpb.RequestVoteRequest{
		Term:        1,
		CandidateId: "node2",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := transport2.SendRequestVote(ctx, addr1, req)
	require.Error(t, err, "should timeout")
}

func TestGRPCTransport_ErrorHandling_NonExistentPeer(t *testing.T) {
	transport, _ := createTestTransport(t)
	transport.RegisterRaftHandler(&mockRaftHandler{})

	require.NoError(t, transport.Start())
	defer transport.Stop()

	// Try to send to non-existent peer
	req := &raftpb.RequestVoteRequest{
		Term:        1,
		CandidateId: "node1",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := transport.SendRequestVote(ctx, "127.0.0.1:99999", req)
	require.Error(t, err, "should fail for non-existent peer")
}

func TestGRPCTransport_ConcurrentRPCs(t *testing.T) {
	transport1, addr1 := createTestTransport(t)
	transport2, _ := createTestTransport(t)

	var callCount atomic.Int32
	handler1 := &mockRaftHandler{
		requestVoteHandler: func(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
			callCount.Add(1)
			// Small delay to ensure concurrent execution
			time.Sleep(10 * time.Millisecond)
			return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
	}
	transport1.RegisterRaftHandler(handler1)
	transport2.RegisterRaftHandler(&mockRaftHandler{})

	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	defer transport1.Stop()
	defer transport2.Stop()

	time.Sleep(50 * time.Millisecond)

	// Send 50 concurrent RPCs
	numRPCs := 50
	var wg sync.WaitGroup
	errors := make(chan error, numRPCs)

	for i := 0; i < numRPCs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			req := &raftpb.RequestVoteRequest{
				Term:        int64(idx),
				CandidateId: fmt.Sprintf("node%d", idx),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := transport2.SendRequestVote(ctx, addr1, req)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Collect any errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	assert.Empty(t, errs, "all concurrent RPCs should succeed")
	assert.Equal(t, int32(numRPCs), callCount.Load(), "all RPCs should be handled")
}

func TestGRPCTransport_NoHandlerRegistered(t *testing.T) {
	transport1, addr1 := createTestTransport(t)
	transport2, _ := createTestTransport(t)

	// Don't register handler on transport1
	transport2.RegisterRaftHandler(&mockRaftHandler{})

	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	defer transport1.Stop()
	defer transport2.Stop()

	time.Sleep(50 * time.Millisecond)

	req := &raftpb.RequestVoteRequest{
		Term:        1,
		CandidateId: "node2",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := transport2.SendRequestVote(ctx, addr1, req)
	require.Error(t, err, "should fail when no handler registered")
}

func TestGRPCTransport_MultiplePeers(t *testing.T) {
	// Create 3 transports
	transport1, _ := createTestTransport(t)
	transport2, addr2 := createTestTransport(t)
	transport3, addr3 := createTestTransport(t)

	var received1, received2, received3 atomic.Int32
	transport1.RegisterRaftHandler(&mockRaftHandler{
		requestVoteHandler: func(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
			received1.Add(1)
			return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
	})
	transport2.RegisterRaftHandler(&mockRaftHandler{
		requestVoteHandler: func(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
			received2.Add(1)
			return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
	})
	transport3.RegisterRaftHandler(&mockRaftHandler{
		requestVoteHandler: func(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
			received3.Add(1)
			return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
	})

	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	require.NoError(t, transport3.Start())
	defer transport1.Stop()
	defer transport2.Stop()
	defer transport3.Stop()

	time.Sleep(50 * time.Millisecond)

	// transport1 sends to transport2 and transport3
	peers := []string{addr2, addr3}
	req := &raftpb.RequestVoteRequest{
		Term:        5,
		CandidateId: "node1",
	}

	for _, peer := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := transport1.SendRequestVote(ctx, peer, req)
		cancel()
		require.NoError(t, err)
		assert.True(t, resp.VoteGranted)
	}

	// Verify each peer received exactly one request
	assert.Equal(t, int32(0), received1.Load(), "transport1 shouldn't receive (it's the sender)")
	assert.Equal(t, int32(1), received2.Load())
	assert.Equal(t, int32(1), received3.Load())

	// Verify transport1 has connections to both peers
	assert.Equal(t, 2, transport1.GetConnectionCount())
}

func TestGRPCTransport_CloseConnection(t *testing.T) {
	transport1, addr1 := createTestTransport(t)
	transport2, _ := createTestTransport(t)

	transport1.RegisterRaftHandler(&mockRaftHandler{})
	transport2.RegisterRaftHandler(&mockRaftHandler{})

	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	defer transport1.Stop()
	defer transport2.Stop()

	time.Sleep(50 * time.Millisecond)

	// Make a request to establish connection
	req := &raftpb.RequestVoteRequest{Term: 1, CandidateId: "node2"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := transport2.SendRequestVote(ctx, addr1, req)
	cancel()
	require.NoError(t, err)

	assert.Equal(t, 1, transport2.GetConnectionCount())

	// Close the connection
	transport2.CloseConnection(addr1)
	assert.Equal(t, 0, transport2.GetConnectionCount())

	// Should be able to reconnect
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	_, err = transport2.SendRequestVote(ctx, addr1, req)
	cancel()
	require.NoError(t, err)
	assert.Equal(t, 1, transport2.GetConnectionCount())
}

func TestGRPCTransport_GracefulShutdown(t *testing.T) {
	transport1, addr1 := createTestTransport(t)
	transport2, addr2 := createTestTransport(t)
	_ = addr2 // addr2 not used in this test but keeps pattern consistent

	var inProgress atomic.Bool
	var completed atomic.Bool

	handler1 := &mockRaftHandler{
		requestVoteHandler: func(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
			inProgress.Store(true)
			defer completed.Store(true)
			// Simulate some work
			time.Sleep(200 * time.Millisecond)
			return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
	}
	transport1.RegisterRaftHandler(handler1)
	transport2.RegisterRaftHandler(&mockRaftHandler{})

	require.NoError(t, transport1.Start())
	require.NoError(t, transport2.Start())
	defer transport2.Stop()

	time.Sleep(50 * time.Millisecond)

	// Start an RPC in background
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := &raftpb.RequestVoteRequest{Term: 1, CandidateId: "node2"}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		transport2.SendRequestVote(ctx, addr1, req)
	}()

	// Wait for request to start processing
	time.Sleep(50 * time.Millisecond)
	require.True(t, inProgress.Load(), "request should be in progress")

	// Stop transport1 while request is in progress
	err := transport1.Stop()
	require.NoError(t, err)

	// Wait for background goroutine
	<-done

	// The graceful shutdown should have allowed the request to complete
	// or should have cancelled it cleanly
}
