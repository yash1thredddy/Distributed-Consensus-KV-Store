package transport

import (
	"context"

	"github.com/yourusername/distributed-kv/api/proto/raftpb"
)

// Transport defines the interface for Raft node communication.
// It provides methods to send RPCs to peers and register handlers
// for incoming RPCs.
type Transport interface {
	// SendRequestVote sends a RequestVote RPC to a specific peer.
	SendRequestVote(ctx context.Context, peer string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)

	// SendAppendEntries sends an AppendEntries RPC to a specific peer.
	SendAppendEntries(ctx context.Context, peer string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)

	// SendInstallSnapshot sends an InstallSnapshot RPC to a specific peer.
	SendInstallSnapshot(ctx context.Context, peer string, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)

	// RegisterRaftHandler registers a handler for incoming Raft RPCs.
	RegisterRaftHandler(handler RaftHandler)

	// Start starts the transport server.
	Start() error

	// Stop stops the transport and cleans up resources.
	Stop() error
}

// RaftHandler defines the interface that Raft nodes must implement
// to handle incoming RPCs.
type RaftHandler interface {
	// HandleRequestVote handles an incoming RequestVote RPC.
	HandleRequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)

	// HandleAppendEntries handles an incoming AppendEntries RPC.
	HandleAppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)

	// HandleInstallSnapshot handles an incoming InstallSnapshot RPC.
	HandleInstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}
