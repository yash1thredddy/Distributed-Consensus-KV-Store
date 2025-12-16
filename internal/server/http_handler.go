package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/raft"
)

// MaxRequestBodySize is the maximum size of request bodies (1MB).
const MaxRequestBodySize = 1 << 20 // 1MB

// HTTPHandler provides REST API handlers for the KV server.
type HTTPHandler struct {
	kv            *KVServer
	raftNode      *raft.RaftNode
	peerHTTPAddrs map[string]string // Maps node ID to HTTP address for forwarding
	httpClient    *http.Client      // Client for forwarding requests
}

// HTTPHandlerConfig holds configuration for the HTTP handler.
type HTTPHandlerConfig struct {
	KV            *KVServer
	RaftNode      *raft.RaftNode
	PeerHTTPAddrs map[string]string // Optional: enables leader forwarding if set
}

// NewHTTPHandler creates a new HTTP handler.
func NewHTTPHandler(kv *KVServer, raftNode *raft.RaftNode) *HTTPHandler {
	return NewHTTPHandlerWithConfig(&HTTPHandlerConfig{
		KV:       kv,
		RaftNode: raftNode,
	})
}

// NewHTTPHandlerWithConfig creates a new HTTP handler with full configuration.
func NewHTTPHandlerWithConfig(cfg *HTTPHandlerConfig) *HTTPHandler {
	return &HTTPHandler{
		kv:            cfg.KV,
		raftNode:      cfg.RaftNode,
		peerHTTPAddrs: cfg.PeerHTTPAddrs,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// RegisterRoutes registers the HTTP routes with the given mux.
func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/kv/", h.handleKV)
	mux.HandleFunc("/cluster/info", h.handleClusterInfo)
	mux.HandleFunc("/health", h.handleHealth)
	mux.Handle("/metrics", promhttp.Handler())
}

// Response types

// ErrorResponse represents an error response.
type ErrorResponse struct {
	Error    string `json:"error"`
	LeaderID string `json:"leader_id,omitempty"`
}

// GetResponse represents a GET response.
type GetResponse struct {
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	Found    bool   `json:"found"`
	Encoding string `json:"encoding,omitempty"` // "base64" if value is binary, empty for UTF-8 text
}

// PutResponse represents a PUT response.
type PutResponse struct {
	Success bool `json:"success"`
}

// DeleteResponse represents a DELETE response.
type DeleteResponse struct {
	Success bool `json:"success"`
}

// ClusterInfoResponse represents cluster status.
type ClusterInfoResponse struct {
	LeaderID string     `json:"leader_id"`
	NodeID   string     `json:"node_id"`
	State    string     `json:"state"`
	Term     int64      `json:"term"`
	Nodes    []NodeInfo `json:"nodes,omitempty"`
}

// NodeInfo represents info about a single node.
type NodeInfo struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	Term        int64  `json:"term"`
	CommitIndex int64  `json:"commit_index"`
}

// HealthResponse represents health check response.
type HealthResponse struct {
	Status   string `json:"status"`
	IsLeader bool   `json:"is_leader"`
	LeaderID string `json:"leader_id"`
}

// handleKV handles /kv/{key} requests.
func (h *HTTPHandler) handleKV(w http.ResponseWriter, r *http.Request) {
	// Extract key from path
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		h.writeError(w, http.StatusBadRequest, "key is required", "")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r, key)
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodDelete:
		h.handleDelete(w, r, key)
	default:
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed", "")
	}
}

// handleGet handles GET /kv/{key}.
func (h *HTTPHandler) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	// Check for linearizable parameter
	linearizable := r.URL.Query().Get("linearizable") == "true"

	value, found, err := h.kv.Get(r.Context(), key, linearizable)
	if err != nil {
		h.handleError(w, err)
		return
	}

	resp := GetResponse{
		Key:   key,
		Found: found,
	}
	if found {
		// Use base64 encoding for binary data, plain string for valid UTF-8
		if utf8.Valid(value) {
			resp.Value = string(value)
		} else {
			resp.Value = base64.StdEncoding.EncodeToString(value)
			resp.Encoding = "base64"
		}
	}

	h.writeJSON(w, http.StatusOK, resp)
}

// handlePut handles PUT /kv/{key}.
func (h *HTTPHandler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	// Limit body size to prevent DoS attacks
	r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBodySize)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		// Check if it's a size limit error
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			h.writeError(w, http.StatusRequestEntityTooLarge, "request body too large", "")
			return
		}
		h.writeError(w, http.StatusBadRequest, "failed to read body", "")
		return
	}

	err = h.kv.Put(r.Context(), key, body)
	if err != nil {
		// Try to forward to leader if we're not the leader and forwarding is enabled
		if h.tryForwardToLeader(w, r, body, err) {
			return
		}
		h.handleError(w, err)
		return
	}

	h.writeJSON(w, http.StatusOK, PutResponse{Success: true})
}

// handleDelete handles DELETE /kv/{key}.
func (h *HTTPHandler) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	err := h.kv.Delete(r.Context(), key)
	if err != nil {
		// Try to forward to leader if we're not the leader and forwarding is enabled
		if h.tryForwardToLeader(w, r, nil, err) {
			return
		}
		h.handleError(w, err)
		return
	}

	h.writeJSON(w, http.StatusOK, DeleteResponse{Success: true})
}

// tryForwardToLeader attempts to forward a request to the leader if:
// 1. The error is a "not leader" error
// 2. We have the leader's HTTP address configured
// Returns true if the request was forwarded (response already written), false otherwise.
func (h *HTTPHandler) tryForwardToLeader(w http.ResponseWriter, r *http.Request, body []byte, err error) bool {
	// Check if it's a not-leader error
	var notLeaderErr *ErrNotLeaderWithHint
	if !errors.As(err, &notLeaderErr) {
		return false
	}

	// Check if we have forwarding configured and know the leader
	if h.peerHTTPAddrs == nil || notLeaderErr.LeaderID == "" {
		return false
	}

	leaderHTTPAddr, ok := h.peerHTTPAddrs[notLeaderErr.LeaderID]
	if !ok || leaderHTTPAddr == "" {
		return false
	}

	// Build the forward URL
	forwardURL := fmt.Sprintf("http://%s%s", leaderHTTPAddr, r.URL.Path)
	if r.URL.RawQuery != "" {
		forwardURL += "?" + r.URL.RawQuery
	}

	// Create the forwarded request
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}

	forwardReq, err := http.NewRequestWithContext(r.Context(), r.Method, forwardURL, reqBody)
	if err != nil {
		return false
	}

	// Copy relevant headers
	forwardReq.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	// Add header to indicate this is a forwarded request (prevent infinite loops)
	forwardReq.Header.Set("X-Forwarded-From", h.raftNode.ID())

	// Execute the forwarded request
	resp, err := h.httpClient.Do(forwardReq)
	if err != nil {
		// Forwarding failed, fall back to returning the not-leader error
		return false
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Copy status code and body
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)

	return true
}

// handleClusterInfo handles GET /cluster/info.
func (h *HTTPHandler) handleClusterInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed", "")
		return
	}

	resp := ClusterInfoResponse{
		LeaderID: h.raftNode.LeaderID(),
		NodeID:   h.raftNode.ID(),
		State:    h.raftNode.State().String(),
		Term:     h.raftNode.Term(),
	}

	h.writeJSON(w, http.StatusOK, resp)
}

// handleHealth handles GET /health.
func (h *HTTPHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed", "")
		return
	}

	resp := HealthResponse{
		Status:   "ok",
		IsLeader: h.raftNode.IsLeader(),
		LeaderID: h.raftNode.LeaderID(),
	}

	h.writeJSON(w, http.StatusOK, resp)
}

// handleError handles errors and returns appropriate HTTP responses.
func (h *HTTPHandler) handleError(w http.ResponseWriter, err error) {
	var notLeaderErr *ErrNotLeaderWithHint
	if errors.As(err, &notLeaderErr) {
		// Set leader hint headers for client to follow
		if notLeaderErr.LeaderAddr != "" {
			w.Header().Set("X-Raft-Leader-ID", notLeaderErr.LeaderID)
			w.Header().Set("X-Raft-Leader-Addr", notLeaderErr.LeaderAddr)
		}
		// Use 503 Service Unavailable instead of 307 Temporary Redirect.
		// 307 requires a Location header which would need a full URL.
		// 503 is more appropriate for "this node can't serve, try leader" semantics.
		// Clients can use the X-Raft-Leader-Addr header to find the leader.
		h.writeError(w, http.StatusServiceUnavailable, notLeaderErr.Error(), notLeaderErr.LeaderID)
		return
	}

	if errors.Is(err, ErrTimeout) {
		h.writeError(w, http.StatusGatewayTimeout, "operation timed out", "")
		return
	}

	if errors.Is(err, ErrServerClosed) {
		h.writeError(w, http.StatusServiceUnavailable, "server is shutting down", "")
		return
	}

	// Generic error
	h.writeError(w, http.StatusInternalServerError, err.Error(), "")
}

// writeJSON writes a JSON response.
func (h *HTTPHandler) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError writes an error response.
func (h *HTTPHandler) writeError(w http.ResponseWriter, status int, message, leaderID string) {
	resp := ErrorResponse{
		Error:    message,
		LeaderID: leaderID,
	}
	h.writeJSON(w, status, resp)
}
