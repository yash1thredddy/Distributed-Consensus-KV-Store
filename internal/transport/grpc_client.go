package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/api/proto/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// getOrCreateConnection returns an existing connection to the peer
// or creates a new one if none exists or the existing one is unhealthy.
func (t *GRPCTransport) getOrCreateConnection(peer string) (*grpc.ClientConn, error) {
	// Fast path: check for existing healthy connection
	t.connMu.RLock()
	conn, exists := t.conns[peer]
	t.connMu.RUnlock()

	if exists && isConnHealthy(conn) {
		return conn, nil
	}

	// Slow path: need to create or replace connection
	t.connMu.Lock()
	defer t.connMu.Unlock()

	// Double-check after acquiring write lock
	conn, exists = t.conns[peer]
	if exists && isConnHealthy(conn) {
		return conn, nil
	}

	// Close existing unhealthy connection if any
	if exists && conn != nil {
		conn.Close()
		delete(t.conns, peer)
	}

	// Create new connection
	newConn, err := t.createConnection(peer)
	if err != nil {
		return nil, err
	}

	t.conns[peer] = newConn
	return newConn, nil
}

// isConnHealthy checks if a gRPC connection is healthy.
func isConnHealthy(conn *grpc.ClientConn) bool {
	if conn == nil {
		return false
	}
	state := conn.GetState()
	// Consider Ready, Idle, and Connecting as healthy
	// TransientFailure and Shutdown are unhealthy
	return state == connectivity.Ready || state == connectivity.Idle || state == connectivity.Connecting
}

// createConnection creates a new gRPC connection to the peer.
func (t *GRPCTransport) createConnection(peer string) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                DefaultKeepaliveTime,
			Timeout:             DefaultKeepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64 * 1024 * 1024), // 64MB for snapshots
			grpc.MaxCallSendMsgSize(64 * 1024 * 1024),
		),
	}

	// Use background context for dial - we don't want the dial to be cancelled
	// by a short RPC timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, peer, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %w", peer, err)
	}

	return conn, nil
}

// SendRequestVote sends a RequestVote RPC to the specified peer.
func (t *GRPCTransport) SendRequestVote(ctx context.Context, peer string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	conn, err := t.getOrCreateConnection(peer)
	if err != nil {
		return nil, err
	}

	client := raftpb.NewRaftServiceClient(conn)

	// Ensure context has a timeout
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultRPCTimeout)
		defer cancel()
	}

	resp, err := client.RequestVote(ctx, req)
	if err != nil {
		// Mark connection as potentially unhealthy on error
		t.handleRPCError(peer, err)
		return nil, fmt.Errorf("RequestVote RPC to %s failed: %w", peer, err)
	}

	return resp, nil
}

// SendAppendEntries sends an AppendEntries RPC to the specified peer.
func (t *GRPCTransport) SendAppendEntries(ctx context.Context, peer string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	conn, err := t.getOrCreateConnection(peer)
	if err != nil {
		return nil, err
	}

	client := raftpb.NewRaftServiceClient(conn)

	// Ensure context has a timeout
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultRPCTimeout)
		defer cancel()
	}

	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		t.handleRPCError(peer, err)
		return nil, fmt.Errorf("AppendEntries RPC to %s failed: %w", peer, err)
	}

	return resp, nil
}

// SendInstallSnapshot sends an InstallSnapshot RPC to the specified peer.
func (t *GRPCTransport) SendInstallSnapshot(ctx context.Context, peer string, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	conn, err := t.getOrCreateConnection(peer)
	if err != nil {
		return nil, err
	}

	client := raftpb.NewRaftServiceClient(conn)

	// Use longer timeout for snapshots
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultSnapshotTimeout)
		defer cancel()
	}

	resp, err := client.InstallSnapshot(ctx, req)
	if err != nil {
		t.handleRPCError(peer, err)
		return nil, fmt.Errorf("InstallSnapshot RPC to %s failed: %w", peer, err)
	}

	return resp, nil
}

// handleRPCError handles RPC errors and may close unhealthy connections.
func (t *GRPCTransport) handleRPCError(peer string, err error) {
	// For simplicity, we just let the next RPC attempt recreate the connection
	// if the existing one is unhealthy. The isConnHealthy check in
	// getOrCreateConnection will handle this.
	//
	// In a more sophisticated implementation, we might:
	// - Track error counts per peer
	// - Implement exponential backoff
	// - Proactively close connections after repeated failures
}

// CloseConnection closes the connection to a specific peer.
func (t *GRPCTransport) CloseConnection(peer string) {
	t.connMu.Lock()
	defer t.connMu.Unlock()

	if conn, exists := t.conns[peer]; exists {
		conn.Close()
		delete(t.conns, peer)
	}
}

// GetConnectionCount returns the number of active connections (for testing).
func (t *GRPCTransport) GetConnectionCount() int {
	t.connMu.RLock()
	defer t.connMu.RUnlock()
	return len(t.conns)
}
