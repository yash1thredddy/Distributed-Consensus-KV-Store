package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yourusername/distributed-kv/api/proto/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Default timeouts and limits
const (
	DefaultRPCTimeout        = 100 * time.Millisecond
	DefaultSnapshotTimeout   = 30 * time.Second
	DefaultKeepaliveTime     = 10 * time.Second
	DefaultKeepaliveTimeout  = 3 * time.Second
	DefaultMaxConcurrentRPCs = 100
)

// GRPCTransport implements the Transport interface using gRPC.
type GRPCTransport struct {
	raftpb.UnimplementedRaftServiceServer

	mu      sync.RWMutex
	addr    string
	server  *grpc.Server
	handler RaftHandler

	// Connection pool to peers
	connMu sync.RWMutex
	conns  map[string]*grpc.ClientConn

	// Lifecycle
	started bool
	stopCh  chan struct{}
}

// GRPCTransportConfig holds configuration for the gRPC transport.
type GRPCTransportConfig struct {
	Addr             string
	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration
	MaxConcurrentRPC int
}

// DefaultGRPCTransportConfig returns default configuration.
func DefaultGRPCTransportConfig(addr string) *GRPCTransportConfig {
	return &GRPCTransportConfig{
		Addr:             addr,
		KeepaliveTime:    DefaultKeepaliveTime,
		KeepaliveTimeout: DefaultKeepaliveTimeout,
		MaxConcurrentRPC: DefaultMaxConcurrentRPCs,
	}
}

// NewGRPCTransport creates a new gRPC transport instance.
func NewGRPCTransport(cfg *GRPCTransportConfig) *GRPCTransport {
	return &GRPCTransport{
		addr:   cfg.Addr,
		conns:  make(map[string]*grpc.ClientConn),
		stopCh: make(chan struct{}),
	}
}

// RegisterRaftHandler registers the Raft handler for incoming RPCs.
func (t *GRPCTransport) RegisterRaftHandler(handler RaftHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = handler
}

// Start starts the gRPC server.
func (t *GRPCTransport) Start() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.started {
		return fmt.Errorf("transport already started")
	}

	// Create TCP listener
	lis, err := net.Listen("tcp", t.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", t.addr, err)
	}

	// Configure server options
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    DefaultKeepaliveTime,
			Timeout: DefaultKeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxConcurrentStreams(uint32(DefaultMaxConcurrentRPCs)),
	}

	// Create gRPC server
	t.server = grpc.NewServer(opts...)
	raftpb.RegisterRaftServiceServer(t.server, t)

	t.started = true

	// Start serving in a goroutine
	go func() {
		if err := t.server.Serve(lis); err != nil {
			// Only log if not stopped gracefully
			select {
			case <-t.stopCh:
				// Expected shutdown
			default:
				// Unexpected error - in production, we'd log this
			}
		}
	}()

	return nil
}

// Stop stops the gRPC server and closes all connections.
func (t *GRPCTransport) Stop() error {
	t.mu.Lock()
	if !t.started {
		t.mu.Unlock()
		return nil
	}
	t.started = false
	t.mu.Unlock()

	// Signal stop
	close(t.stopCh)

	// Graceful stop with timeout
	if t.server != nil {
		stopped := make(chan struct{})
		go func() {
			t.server.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
			// Graceful shutdown completed
		case <-time.After(5 * time.Second):
			// Force stop
			t.server.Stop()
		}
	}

	// Close all client connections
	t.connMu.Lock()
	for addr, conn := range t.conns {
		conn.Close()
		delete(t.conns, addr)
	}
	t.connMu.Unlock()

	return nil
}

// RaftServiceServer implementation - delegates to registered handler

// RequestVote handles incoming RequestVote RPCs.
func (t *GRPCTransport) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()

	if handler == nil {
		return nil, status.Error(codes.Unavailable, "no handler registered")
	}

	return handler.HandleRequestVote(ctx, req)
}

// AppendEntries handles incoming AppendEntries RPCs.
func (t *GRPCTransport) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()

	if handler == nil {
		return nil, status.Error(codes.Unavailable, "no handler registered")
	}

	return handler.HandleAppendEntries(ctx, req)
}

// InstallSnapshot handles incoming InstallSnapshot RPCs.
func (t *GRPCTransport) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()

	if handler == nil {
		return nil, status.Error(codes.Unavailable, "no handler registered")
	}

	return handler.HandleInstallSnapshot(ctx, req)
}

// Ensure GRPCTransport implements Transport interface
var _ Transport = (*GRPCTransport)(nil)
