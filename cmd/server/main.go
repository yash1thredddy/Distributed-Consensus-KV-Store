package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/raft"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/server"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/storage"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/internal/transport"
	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/pkg/config"
)

func main() {
	configPath := flag.String("config", "", "Path to configuration file")
	flag.Parse()

	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		logger.Fatal("Invalid configuration", zap.Error(err))
	}

	logger.Info("Starting distributed KV store node",
		zap.String("nodeID", cfg.NodeID),
		zap.String("raftAddr", cfg.RaftAddr),
		zap.String("httpAddr", cfg.HTTPAddr),
		zap.String("dataDir", cfg.DataDir),
	)

	// Parse peer addresses into IDs and address map
	// Peers are in format "hostname:port" (e.g., "node2:5001")
	// We extract the hostname as the peer ID
	peerIDs, peerAddrs := parsePeers(cfg.Peers, cfg.NodeID, cfg.RaftAddr)

	logger.Info("Peer configuration",
		zap.Strings("peerIDs", peerIDs),
		zap.Any("peerAddrs", peerAddrs),
	)

	// Initialize storage
	store, err := storage.NewBadgerStorage(storage.DefaultBadgerConfig(cfg.DataDir))
	if err != nil {
		logger.Fatal("Failed to create storage", zap.Error(err))
	}
	defer store.Close()

	// Initialize transport
	transportCfg := transport.DefaultGRPCTransportConfig(cfg.RaftAddr)
	trans := transport.NewGRPCTransport(transportCfg)

	// Create apply channel
	applyCh := make(chan raft.ApplyMsg, 100)

	// Build Raft configuration
	raftCfg, err := raft.NewRaftConfigBuilder(cfg.NodeID).
		WithPeers(peerIDs).
		WithPeerAddrs(peerAddrs).
		WithStorage(store).
		WithTransport(trans).
		WithApplyCh(applyCh).
		WithElectionTimeout(cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax).
		WithHeartbeatInterval(cfg.HeartbeatInterval).
		WithLogger(logger).
		Build()
	if err != nil {
		logger.Fatal("Failed to build Raft config", zap.Error(err))
	}

	// Create Raft node
	raftNode, err := raft.NewRaftNode(raftCfg)
	if err != nil {
		logger.Fatal("Failed to create Raft node", zap.Error(err))
	}

	// Create KV server
	kvServer := server.NewKVServer(&server.KVServerConfig{
		Raft:             raftNode,
		Storage:          store,
		OperationTimeout: 10 * time.Second,
	})

	// Create HTTP handler with leader forwarding support
	// Build peer HTTP addresses map if configured, or derive from peer Raft addresses
	peerHTTPAddrs := cfg.PeerHTTPAddrs
	if peerHTTPAddrs == nil {
		// Auto-derive HTTP addresses: use same hostname as Raft, port 8080
		// This works for Docker where all nodes use :8080 internally
		peerHTTPAddrs = derivePeerHTTPAddrs(cfg.Peers, cfg.NodeID, cfg.HTTPAddr)
	}
	httpHandler := server.NewHTTPHandlerWithConfig(&server.HTTPHandlerConfig{
		KV:            kvServer,
		RaftNode:      raftNode,
		PeerHTTPAddrs: peerHTTPAddrs,
	})

	// Create WebSocket handler
	wsHandler := server.NewWebSocketHandler(raftNode)

	// Set up HTTP mux
	mux := http.NewServeMux()
	httpHandler.RegisterRoutes(mux)
	mux.HandleFunc("/ws/cluster", wsHandler.HandleWebSocket)

	// Create HTTP server
	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start components
	logger.Info("Starting transport...")
	if err := trans.Start(); err != nil {
		logger.Fatal("Failed to start transport", zap.Error(err))
	}

	// Small delay to ensure transport is ready
	time.Sleep(100 * time.Millisecond)

	logger.Info("Starting Raft node...")
	if err := raftNode.Start(); err != nil {
		trans.Stop()
		logger.Fatal("Failed to start Raft node", zap.Error(err))
	}

	logger.Info("Starting KV server...")
	if err := kvServer.Start(); err != nil {
		raftNode.Stop()
		trans.Stop()
		logger.Fatal("Failed to start KV server", zap.Error(err))
	}

	logger.Info("Starting WebSocket handler...")
	wsHandler.Start()

	// Start HTTP server in a goroutine
	go func() {
		logger.Info("Starting HTTP server", zap.String("addr", cfg.HTTPAddr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	logger.Info("Server started successfully",
		zap.String("nodeID", cfg.NodeID),
		zap.String("httpAddr", cfg.HTTPAddr),
	)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown HTTP server
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	// Stop WebSocket handler
	wsHandler.Stop()

	// Stop KV server
	if err := kvServer.Stop(); err != nil {
		logger.Error("KV server stop error", zap.Error(err))
	}

	// Stop Raft node
	raftNode.Stop()

	// Stop transport
	trans.Stop()

	logger.Info("Server stopped")
}

// parsePeers parses peer addresses and builds peer IDs and address map.
// Input format: ["node2:5001", "node3:5001", ...]
// Returns: peerIDs ["node2", "node3", ...], peerAddrs {"node1": ":5001", "node2": "node2:5001", ...}
func parsePeers(peers []string, selfID, selfAddr string) ([]string, map[string]string) {
	peerIDs := make([]string, 0, len(peers))
	peerAddrs := make(map[string]string)

	// Add self to address map
	peerAddrs[selfID] = selfAddr

	for _, peer := range peers {
		// Parse "hostname:port" format
		parts := strings.SplitN(peer, ":", 2)
		if len(parts) != 2 {
			continue
		}
		peerID := parts[0]
		peerIDs = append(peerIDs, peerID)
		peerAddrs[peerID] = peer
	}

	return peerIDs, peerAddrs
}

// derivePeerHTTPAddrs derives HTTP addresses from peer Raft addresses.
// For Docker deployments, peers use the same hostname with HTTP port 8080.
// Input format: ["node2:5001", "node3:5001", ...]
// Returns: {"node1": "node1:8080", "node2": "node2:8080", ...}
func derivePeerHTTPAddrs(peers []string, selfID, selfHTTPAddr string) map[string]string {
	httpAddrs := make(map[string]string)

	// Extract port from self HTTP address (e.g., ":8080" -> "8080")
	httpPort := "8080"
	if idx := strings.LastIndex(selfHTTPAddr, ":"); idx != -1 {
		httpPort = selfHTTPAddr[idx+1:]
	}

	// Add self - use hostname from selfID for consistency in Docker
	httpAddrs[selfID] = selfID + ":" + httpPort

	for _, peer := range peers {
		// Parse "hostname:port" format
		parts := strings.SplitN(peer, ":", 2)
		if len(parts) != 2 {
			continue
		}
		peerID := parts[0]
		// Use peer hostname with HTTP port
		httpAddrs[peerID] = peerID + ":" + httpPort
	}

	return httpAddrs
}
