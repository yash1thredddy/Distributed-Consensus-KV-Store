package raft

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourusername/distributed-kv/api/proto/raftpb"
)

// electionTimer runs the election timeout loop.
// When timeout expires without hearing from leader, starts an election.
func (rn *RaftNode) electionTimer() {
	defer rn.wg.Done()

	for {
		// Randomize timeout each iteration
		timeout := rn.randomElectionTimeout()

		select {
		case <-time.After(timeout):
			rn.mu.Lock()
			if rn.state != Leader {
				rn.mu.Unlock()
				rn.startElection()
			} else {
				rn.mu.Unlock()
			}

		case <-rn.resetTimerCh:
			// Timer reset, continue to next iteration
			continue

		case <-rn.stopCh:
			return
		}
	}
}

// randomElectionTimeout returns a random duration between min and max.
func (rn *RaftNode) randomElectionTimeout() time.Duration {
	diff := rn.electionTimeoutMax - rn.electionTimeoutMin
	return rn.electionTimeoutMin + time.Duration(rand.Int63n(int64(diff)))
}

// startElection transitions to candidate and starts a new election.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()

	// Transition to candidate
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id // Vote for self
	currentTerm := rn.currentTerm
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()
	peers := make([]string, len(rn.peers))
	copy(peers, rn.peers)

	// Persist state before sending RequestVote
	if err := rn.persistState(); err != nil {
		// Failed to persist, abort election
		rn.state = Follower
		rn.mu.Unlock()
		return
	}

	rn.mu.Unlock()

	// Reset election timer
	rn.resetElectionTimer()

	// Count votes (start with self-vote)
	var votesReceived atomic.Int32
	votesReceived.Store(1)

	// Send RequestVote to all peers in parallel
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			rn.sendRequestVote(peerID, currentTerm, lastLogIndex, lastLogTerm, &votesReceived)
		}(peer)
	}

	// Wait for all RPCs to complete (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(rn.electionTimeoutMin):
		// Timeout waiting for votes
	case <-rn.stopCh:
		return
	}

	// Check if we won
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Verify we're still candidate and term hasn't changed
	if rn.state != Candidate || rn.currentTerm != currentTerm {
		return
	}

	if int(votesReceived.Load()) >= rn.quorumSize() {
		rn.becomeLeader()
	}
}

// sendRequestVote sends a RequestVote RPC to a single peer.
func (rn *RaftNode) sendRequestVote(peerID string, term, lastLogIndex, lastLogTerm int64, votesReceived *atomic.Int32) {
	peerAddr := rn.GetPeerAddr(peerID)
	if peerAddr == "" {
		return
	}

	req := &raftpb.RequestVoteRequest{
		Term:         term,
		CandidateId:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()

	resp, err := rn.transport.SendRequestVote(ctx, peerAddr, req)
	if err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Check if response is still relevant
	if rn.state != Candidate || rn.currentTerm != term {
		return
	}

	// Step down if we see a higher term
	if resp.Term > rn.currentTerm {
		rn.stepDown(resp.Term)
		return
	}

	if resp.VoteGranted {
		votesReceived.Add(1)

		// Check if we've won
		if int(votesReceived.Load()) >= rn.quorumSize() && rn.state == Candidate {
			rn.becomeLeader()
		}
	}
}

// becomeLeader transitions to leader state and initializes leader state.
// Must be called with mu held.
func (rn *RaftNode) becomeLeader() {
	if rn.state != Candidate {
		return
	}

	rn.state = Leader
	rn.leaderId = rn.id

	// Initialize nextIndex and matchIndex for all peers
	lastIndex := rn.log.LastIndex()
	for _, peer := range rn.peers {
		rn.nextIndex[peer] = lastIndex + 1
		rn.matchIndex[peer] = 0
	}

	// Append no-op entry to commit previous terms' entries
	noopEntry := LogEntry{
		Term:    rn.currentTerm,
		Index:   rn.log.NextIndex(),
		Command: nil, // No-op
	}
	if err := rn.log.Append(noopEntry); err != nil {
		// Failed to append no-op, step down
		rn.stepDown(rn.currentTerm)
		return
	}

	// Start leader goroutine
	rn.wg.Add(1)
	go rn.leaderLoop()

	// Trigger immediate heartbeat
	rn.triggerReplication()
}
