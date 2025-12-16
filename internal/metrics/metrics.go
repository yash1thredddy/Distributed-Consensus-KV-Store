package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// --------------------------------------------------------------------------
// Raft Metrics
// --------------------------------------------------------------------------

var (
	// RaftCurrentTerm tracks the current Raft term.
	RaftCurrentTerm = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "current_term",
		Help:      "Current Raft term",
	})

	// RaftState tracks the node state (0=follower, 1=candidate, 2=leader).
	RaftState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "state",
		Help:      "Current node state (0=follower, 1=candidate, 2=leader)",
	}, []string{"node_id"})

	// RaftCommitIndex tracks the current commit index.
	RaftCommitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "commit_index",
		Help:      "Current commit index",
	})

	// RaftLastApplied tracks the last applied index.
	RaftLastApplied = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "last_applied",
		Help:      "Last applied index",
	})

	// RaftLogEntries tracks the number of log entries.
	RaftLogEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "log_entries",
		Help:      "Number of log entries",
	})

	// RaftElectionCount counts the number of elections started.
	RaftElectionCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "raft",
		Name:      "election_count_total",
		Help:      "Total number of elections started",
	})

	// RaftLeaderChanges counts the number of leader changes observed.
	RaftLeaderChanges = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "raft",
		Name:      "leader_changes_total",
		Help:      "Total number of leader changes observed",
	})

	// RaftIsLeader indicates if this node is the leader (1) or not (0).
	RaftIsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "is_leader",
		Help:      "Whether this node is the leader (1=leader, 0=not leader)",
	})
)

// --------------------------------------------------------------------------
// RPC Metrics
// --------------------------------------------------------------------------

var (
	// RPCDuration tracks RPC latency by type.
	RPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Name:      "rpc_duration_seconds",
		Help:      "RPC latency in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~3.2s
	}, []string{"rpc_type"})

	// RPCErrors counts RPC errors by type and error category.
	RPCErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Name:      "rpc_errors_total",
		Help:      "Total RPC errors",
	}, []string{"rpc_type", "error_type"})

	// AppendEntriesBatchSize tracks the number of entries per AppendEntries RPC.
	AppendEntriesBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "raft",
		Name:      "append_entries_batch_size",
		Help:      "Number of entries per AppendEntries RPC",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1 to 512
	})
)

// --------------------------------------------------------------------------
// Storage Metrics
// --------------------------------------------------------------------------

var (
	// StorageOperations counts storage operations by type.
	StorageOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "storage",
		Name:      "operations_total",
		Help:      "Total storage operations",
	}, []string{"operation"})

	// StorageOperationDuration tracks storage operation latency.
	StorageOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "storage",
		Name:      "operation_duration_seconds",
		Help:      "Storage operation latency in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.00001, 2, 15), // 10us to ~300ms
	}, []string{"operation"})

	// StorageLogSize tracks the size of the Raft log in bytes.
	StorageLogSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "storage",
		Name:      "log_size_bytes",
		Help:      "Size of the Raft log in bytes",
	})
)

// --------------------------------------------------------------------------
// KV Server Metrics
// --------------------------------------------------------------------------

var (
	// KVOperations counts KV operations by type.
	KVOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kv",
		Name:      "operations_total",
		Help:      "Total KV operations",
	}, []string{"operation", "status"})

	// KVOperationDuration tracks KV operation latency.
	KVOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kv",
		Name:      "operation_duration_seconds",
		Help:      "KV operation latency in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~3.2s
	}, []string{"operation"})

	// KVPendingOperations tracks the number of pending operations.
	KVPendingOperations = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "kv",
		Name:      "pending_operations",
		Help:      "Number of pending KV operations waiting for commit",
	})
)

// --------------------------------------------------------------------------
// HTTP/WebSocket Metrics
// --------------------------------------------------------------------------

var (
	// HTTPRequestDuration tracks HTTP request latency.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "http",
		Name:      "request_duration_seconds",
		Help:      "HTTP request latency in seconds",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path", "status_code"})

	// HTTPRequestsTotal counts HTTP requests.
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "http",
		Name:      "requests_total",
		Help:      "Total HTTP requests",
	}, []string{"method", "path", "status_code"})

	// WebSocketConnections tracks active WebSocket connections.
	WebSocketConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "websocket",
		Name:      "connections",
		Help:      "Number of active WebSocket connections",
	})
)

// --------------------------------------------------------------------------
// RPC Type Constants
// --------------------------------------------------------------------------

const (
	RPCTypeRequestVote     = "request_vote"
	RPCTypeAppendEntries   = "append_entries"
	RPCTypeInstallSnapshot = "install_snapshot"
)

// --------------------------------------------------------------------------
// Storage Operation Constants
// --------------------------------------------------------------------------

const (
	StorageOpGet            = "get"
	StorageOpPut            = "put"
	StorageOpDelete         = "delete"
	StorageOpAppendLog      = "append_log"
	StorageOpGetLog         = "get_log"
	StorageOpTruncateLog    = "truncate_log"
	StorageOpSaveState      = "save_state"
	StorageOpLoadState      = "load_state"
	StorageOpSaveSnapshot   = "save_snapshot"
	StorageOpLoadSnapshot   = "load_snapshot"
)

// --------------------------------------------------------------------------
// KV Operation Constants
// --------------------------------------------------------------------------

const (
	KVOpGet    = "get"
	KVOpPut    = "put"
	KVOpDelete = "delete"
)

const (
	KVStatusSuccess = "success"
	KVStatusError   = "error"
)

// --------------------------------------------------------------------------
// Error Type Constants
// --------------------------------------------------------------------------

const (
	ErrorTypeTimeout    = "timeout"
	ErrorTypeConnection = "connection"
	ErrorTypeInternal   = "internal"
	ErrorTypeNotLeader  = "not_leader"
)
