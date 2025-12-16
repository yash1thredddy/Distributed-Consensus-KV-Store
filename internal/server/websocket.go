package server

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yourusername/distributed-kv/internal/raft"
)

// WebSocketHandler handles WebSocket connections for real-time cluster status.
type WebSocketHandler struct {
	raftNode *raft.RaftNode

	// upgrader upgrades HTTP connections to WebSocket
	upgrader websocket.Upgrader

	// clients tracks connected WebSocket clients
	mu      sync.RWMutex
	clients map[*websocket.Conn]bool

	// stopCh signals shutdown
	stopCh chan struct{}
	wg     sync.WaitGroup

	// updateInterval is how often to send updates
	updateInterval time.Duration
}

// ClusterState represents the cluster state sent over WebSocket.
type ClusterState struct {
	LeaderID  string       `json:"leader_id"`
	NodeID    string       `json:"node_id"`
	State     string       `json:"state"`
	Term      int64        `json:"term"`
	Commit    int64        `json:"commit_index"`
	Applied   int64        `json:"last_applied"`
	Timestamp int64        `json:"timestamp"`
	Nodes     []NodeStatus `json:"nodes,omitempty"`
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

// NewWebSocketHandler creates a new WebSocket handler.
func NewWebSocketHandler(raftNode *raft.RaftNode) *WebSocketHandler {
	return &WebSocketHandler{
		raftNode: raftNode,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for development
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		clients:        make(map[*websocket.Conn]bool),
		stopCh:         make(chan struct{}),
		updateInterval: 500 * time.Millisecond,
	}
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
	for conn := range h.clients {
		conn.Close()
	}
	h.clients = make(map[*websocket.Conn]bool)
	h.mu.Unlock()
}

// HandleWebSocket handles WebSocket upgrade and connection.
func (h *WebSocketHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	// Register client
	h.mu.Lock()
	h.clients[conn] = true
	h.mu.Unlock()

	// Send initial state
	state := h.getClusterState()
	h.sendToClient(conn, state)

	// Handle incoming messages (for now, just handle disconnection)
	go h.handleClient(conn)
}

// handleClient handles messages from a WebSocket client.
func (h *WebSocketHandler) handleClient(conn *websocket.Conn) {
	defer func() {
		h.mu.Lock()
		delete(h.clients, conn)
		h.mu.Unlock()
		conn.Close()
	}()

	for {
		// Read message (we don't expect any, but need to handle close)
		_, _, err := conn.ReadMessage()
		if err != nil {
			return
		}
	}
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
		return
	}

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		// Connection error, will be cleaned up by handleClient
		h.mu.Lock()
		delete(h.clients, conn)
		h.mu.Unlock()
		conn.Close()
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
		Nodes: []NodeStatus{
			{
				ID:          h.raftNode.ID(),
				State:       h.raftNode.State().String(),
				Term:        h.raftNode.Term(),
				CommitIndex: h.raftNode.CommitIndex(),
				LastApplied: h.raftNode.LastApplied(),
				IsLeader:    h.raftNode.IsLeader(),
			},
		},
	}
}

// RegisterRoutes registers the WebSocket route with the given mux.
func (h *WebSocketHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/ws/cluster", h.HandleWebSocket)
}
