package server

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/raft"
)

// clientConn wraps a WebSocket connection with cleanup coordination.
type clientConn struct {
	conn      *websocket.Conn
	closeOnce sync.Once
}

// WebSocketHandler handles WebSocket connections for real-time cluster status.
type WebSocketHandler struct {
	raftNode *raft.RaftNode

	// upgrader upgrades HTTP connections to WebSocket
	upgrader websocket.Upgrader

	// clients tracks connected WebSocket clients
	mu      sync.RWMutex
	clients map[*websocket.Conn]*clientConn

	// stopCh signals shutdown
	stopCh chan struct{}
	wg     sync.WaitGroup

	// updateInterval is how often to send updates
	updateInterval time.Duration

	// allowedOrigins for CORS validation (empty means development mode - allow all)
	allowedOrigins []string
}

// ClusterState represents the cluster state sent over WebSocket.
type ClusterState struct {
	LeaderID  string     `json:"leader_id"`
	NodeID    string     `json:"node_id"`
	State     string     `json:"state"`
	Term      int64      `json:"term"`
	Commit    int64      `json:"commit_index"`
	Applied   int64      `json:"last_applied"`
	Timestamp int64      `json:"timestamp"`
	Node      NodeStatus `json:"node"` // Status of the current node
}

// NodeStatus represents the status of a single node.
type NodeStatus struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	Term        int64  `json:"term"`
	CommitIndex int64  `json:"commit_index"`
	LastApplied int64  `json:"last_applied"`
	IsLeader    bool   `json:"is_leader"`
}

// WebSocketHandlerConfig holds configuration for WebSocketHandler.
type WebSocketHandlerConfig struct {
	RaftNode       *raft.RaftNode
	AllowedOrigins []string      // Empty means allow all (development mode)
	UpdateInterval time.Duration // Default: 500ms
}

// NewWebSocketHandler creates a new WebSocket handler.
func NewWebSocketHandler(raftNode *raft.RaftNode) *WebSocketHandler {
	return NewWebSocketHandlerWithConfig(&WebSocketHandlerConfig{
		RaftNode: raftNode,
	})
}

// NewWebSocketHandlerWithConfig creates a new WebSocket handler with custom configuration.
func NewWebSocketHandlerWithConfig(cfg *WebSocketHandlerConfig) *WebSocketHandler {
	updateInterval := cfg.UpdateInterval
	if updateInterval == 0 {
		updateInterval = 500 * time.Millisecond
	}

	h := &WebSocketHandler{
		raftNode:       cfg.RaftNode,
		clients:        make(map[*websocket.Conn]*clientConn),
		stopCh:         make(chan struct{}),
		updateInterval: updateInterval,
		allowedOrigins: cfg.AllowedOrigins,
	}

	h.upgrader = websocket.Upgrader{
		CheckOrigin:     h.checkOrigin,
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	return h
}

// checkOrigin validates the request origin against allowed origins.
func (h *WebSocketHandler) checkOrigin(r *http.Request) bool {
	// If no allowed origins configured, allow all (development mode)
	if len(h.allowedOrigins) == 0 {
		return true
	}

	origin := r.Header.Get("Origin")
	for _, allowed := range h.allowedOrigins {
		if origin == allowed {
			return true
		}
	}
	return false
}

// Start starts the WebSocket handler's background goroutine.
func (h *WebSocketHandler) Start() {
	h.wg.Add(1)
	go h.broadcastLoop()
}

// Stop stops the WebSocket handler.
func (h *WebSocketHandler) Stop() {
	close(h.stopCh)
	h.wg.Wait()

	// Close all client connections
	h.mu.Lock()
	for conn, cc := range h.clients {
		cc.closeOnce.Do(func() {
			conn.Close()
		})
	}
	h.clients = make(map[*websocket.Conn]*clientConn)
	h.mu.Unlock()
}

// HandleWebSocket handles WebSocket upgrade and connection.
func (h *WebSocketHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	cc := &clientConn{conn: conn}

	// Register client
	h.mu.Lock()
	h.clients[conn] = cc
	h.mu.Unlock()

	// Send initial state
	state := h.getClusterState()
	h.sendToClient(conn, state)

	// Handle incoming messages (for now, just handle disconnection)
	go h.handleClient(conn, cc)
}

// handleClient handles messages from a WebSocket client.
func (h *WebSocketHandler) handleClient(conn *websocket.Conn, cc *clientConn) {
	defer h.removeClient(conn, cc)

	for {
		// Read message (we don't expect any, but need to handle close)
		_, _, err := conn.ReadMessage()
		if err != nil {
			return
		}
	}
}

// removeClient safely removes and closes a client connection exactly once.
func (h *WebSocketHandler) removeClient(conn *websocket.Conn, cc *clientConn) {
	cc.closeOnce.Do(func() {
		h.mu.Lock()
		delete(h.clients, conn)
		h.mu.Unlock()
		conn.Close()
	})
}

// broadcastLoop periodically broadcasts cluster state to all clients.
func (h *WebSocketHandler) broadcastLoop() {
	defer h.wg.Done()

	ticker := time.NewTicker(h.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			h.broadcast()
		}
	}
}

// broadcast sends current cluster state to all connected clients.
func (h *WebSocketHandler) broadcast() {
	state := h.getClusterState()

	h.mu.RLock()
	clients := make([]*websocket.Conn, 0, len(h.clients))
	for conn := range h.clients {
		clients = append(clients, conn)
	}
	h.mu.RUnlock()

	for _, conn := range clients {
		h.sendToClient(conn, state)
	}
}

// sendToClient sends a message to a single client.
func (h *WebSocketHandler) sendToClient(conn *websocket.Conn, state ClusterState) {
	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("websocket: failed to marshal cluster state: %v", err)
		return
	}

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		// Connection error - use centralized cleanup
		h.mu.RLock()
		cc, ok := h.clients[conn]
		h.mu.RUnlock()
		if ok {
			h.removeClient(conn, cc)
		}
	}
}

// getClusterState returns the current cluster state.
func (h *WebSocketHandler) getClusterState() ClusterState {
	return ClusterState{
		LeaderID:  h.raftNode.LeaderID(),
		NodeID:    h.raftNode.ID(),
		State:     h.raftNode.State().String(),
		Term:      h.raftNode.Term(),
		Commit:    h.raftNode.CommitIndex(),
		Applied:   h.raftNode.LastApplied(),
		Timestamp: time.Now().UnixMilli(),
		Node: NodeStatus{
			ID:          h.raftNode.ID(),
			State:       h.raftNode.State().String(),
			Term:        h.raftNode.Term(),
			CommitIndex: h.raftNode.CommitIndex(),
			LastApplied: h.raftNode.LastApplied(),
			IsLeader:    h.raftNode.IsLeader(),
		},
	}
}

// RegisterRoutes registers the WebSocket route with the given mux.
func (h *WebSocketHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/ws/cluster", h.HandleWebSocket)
}
