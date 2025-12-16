package transport

import (
	"context"

	"github.com/yourusername/distributed-kv/api/proto/raftpb"
)

// --------------------------------------------------------------------------
// Interface Segregation: Small, focused interfaces for different capabilities
// --------------------------------------------------------------------------

// VoteSender defines the ability to send vote requests.
type VoteSender interface {
	SendRequestVote(ctx context.Context, peer string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
}

// EntriesSender defines the ability to send log entries.
type EntriesSender interface {
	SendAppendEntries(ctx context.Context, peer string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
}

// SnapshotSender defines the ability to send snapshots.
type SnapshotSender interface {
	SendInstallSnapshot(ctx context.Context, peer string, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

// RPCSender combines all RPC sending capabilities.
// Used by: RaftNode for sending all types of RPCs
type RPCSender interface {
	VoteSender
	EntriesSender
	SnapshotSender
}

// HandlerRegistry defines the ability to register RPC handlers.
type HandlerRegistry interface {
	RegisterRaftHandler(handler RaftHandler)
}

// Lifecycle defines start/stop operations.
type Lifecycle interface {
	Start() error
	Stop() error
}

// --------------------------------------------------------------------------
// Composite interfaces
// --------------------------------------------------------------------------

// Transport defines the full interface for Raft node communication.
// Implementations (GRPCTransport) implement this complete interface.
type Transport interface {
	RPCSender
	HandlerRegistry
	Lifecycle
}

// --------------------------------------------------------------------------
// Handler interfaces (for incoming RPCs)
// --------------------------------------------------------------------------

// VoteHandler handles incoming vote requests.
type VoteHandler interface {
	HandleRequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
}

// EntriesHandler handles incoming append entries requests.
type EntriesHandler interface {
	HandleAppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
}

// SnapshotHandler handles incoming install snapshot requests.
type SnapshotHandler interface {
	HandleInstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

// RaftHandler combines all handler interfaces.
// Implemented by: RaftNode
type RaftHandler interface {
	VoteHandler
	EntriesHandler
	SnapshotHandler
}
