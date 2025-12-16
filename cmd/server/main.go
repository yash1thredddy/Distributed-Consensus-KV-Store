package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/pkg/config"
)

func main() {
	configPath := flag.String("config", "", "Path to configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Starting distributed KV store node: %s\n", cfg.NodeID)
	fmt.Printf("Raft address: %s\n", cfg.RaftAddr)
	fmt.Printf("HTTP address: %s\n", cfg.HTTPAddr)
	fmt.Printf("Data directory: %s\n", cfg.DataDir)

	// TODO: Initialize and start server components
	// - Storage layer
	// - Transport layer
	// - Raft node
	// - KV server
	// - HTTP server

	fmt.Println("Server initialization not yet implemented")
}
