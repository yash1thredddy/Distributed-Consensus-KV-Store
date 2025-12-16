package raft

import (
	"context"
	"time"

	"github.com/yourusername/distributed-kv/api/proto/raftpb"
)

// leaderLoop is the main loop for the leader.
// Sends heartbeats and replicates log entries to followers.
func (rn *RaftNode) leaderLoop() {
	defer rn.wg.Done()

	ticker := time.NewTicker(rn.heartbeatInterval)
	defer ticker.Stop()

	// Send initial heartbeat
	rn.sendHeartbeats()

	for {
		select {
		case <-ticker.C:
			rn.sendHeartbeats()

		case <-rn.newEntryCh:
			// New entry, trigger immediate replication
			rn.sendHeartbeats()

		case <-rn.stepDownCh:
			// Leader stepped down
			return

		case <-rn.stopCh:
			return
		}
	}
}

// sendHeartbeats sends AppendEntries to all peers.
func (rn *RaftNode) sendHeartbeats() {
	rn.mu.RLock()
	if rn.state != Leader {
		rn.mu.RUnlock()
		return
	}

	currentTerm := rn.currentTerm
	commitIndex := rn.commitIndex
	peers := make([]string, len(rn.peers))
	copy(peers, rn.peers)
	rn.mu.RUnlock()

	for _, peer := range peers {
		go rn.sendAppendEntries(peer, currentTerm, commitIndex)
	}
}

// sendAppendEntries sends AppendEntries RPC to a single peer.
func (rn *RaftNode) sendAppendEntries(peerID string, currentTerm, commitIndex int64) {
	peerAddr := rn.GetPeerAddr(peerID)
	if peerAddr == "" {
		return
	}

	// Prepare request data under lock
	rn.mu.RLock()

	// Verify still leader
	if rn.state != Leader || rn.currentTerm != currentTerm {
		rn.mu.RUnlock()
		return
	}

	nextIndex := rn.nextIndex[peerID]
	prevLogIndex := nextIndex - 1
	prevLogTerm := rn.log.GetTerm(prevLogIndex)

	// Get entries to send
	lastIndex := rn.log.LastIndex()
	var entries []LogEntry
	if nextIndex <= lastIndex {
		entries = rn.log.GetEntries(nextIndex, lastIndex+1)
	}

	rn.mu.RUnlock()

	// Build request
	req := &raftpb.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     rn.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      convertLogEntriesToProto(entries),
		LeaderCommit: commitIndex,
	}

	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()

	resp, err := rn.transport.SendAppendEntries(ctx, peerAddr, req)
	if err != nil {
		return
	}

	// Process response under lock
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Verify still leader and term hasn't changed
	if rn.state != Leader || rn.currentTerm != currentTerm {
		return
	}

	// Step down if we see higher term
	if resp.Term > rn.currentTerm {
		rn.stepDown(resp.Term)
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex
		newMatchIndex := prevLogIndex + int64(len(entries))
		if newMatchIndex > rn.matchIndex[peerID] {
			rn.matchIndex[peerID] = newMatchIndex
		}
		rn.nextIndex[peerID] = newMatchIndex + 1

		// Check if we can advance commitIndex
		rn.checkCommit()
	} else {
		// Log inconsistency - back up nextIndex
		if resp.ConflictTerm > 0 {
			// Use conflict optimization
			// Find the last entry with conflictTerm
			foundIndex := int64(0)
			for i := rn.log.LastIndex(); i >= 1; i-- {
				if rn.log.GetTerm(i) == resp.ConflictTerm {
					foundIndex = i
					break
				}
			}
			if foundIndex > 0 {
				// We have entries with conflictTerm, next try after them
				rn.nextIndex[peerID] = foundIndex + 1
			} else {
				// We don't have conflictTerm, skip to conflictIndex
				rn.nextIndex[peerID] = resp.ConflictIndex
			}
		} else {
			// No conflict term info, use conflictIndex
			if resp.ConflictIndex > 0 {
				rn.nextIndex[peerID] = resp.ConflictIndex
			} else {
				// Decrement by 1
				if rn.nextIndex[peerID] > 1 {
					rn.nextIndex[peerID]--
				}
			}
		}
	}
}

// checkCommit checks if any new entries can be committed.
// Must be called with mu held.
func (rn *RaftNode) checkCommit() {
	if rn.state != Leader {
		return
	}

	// For each index from commitIndex+1 to lastIndex
	for n := rn.commitIndex + 1; n <= rn.log.LastIndex(); n++ {
		// Only commit entries from current term (Raft safety)
		if rn.log.GetTerm(n) != rn.currentTerm {
			continue
		}

		// Count replicas (including self)
		count := 1
		for _, peer := range rn.peers {
			if rn.matchIndex[peer] >= n {
				count++
			}
		}

		// Check if we have a majority
		if count >= rn.quorumSize() {
			rn.commitIndex = n
		}
	}
}

// applyLoop applies committed entries to the state machine.
func (rn *RaftNode) applyLoop() {
	defer rn.wg.Done()

	for {
		select {
		case <-rn.stopCh:
			return
		default:
		}

		// Check for entries to apply
		rn.mu.Lock()
		var toApply []ApplyMsg

		for rn.lastApplied < rn.commitIndex {
			rn.lastApplied++
			entry := rn.log.GetEntry(rn.lastApplied)
			if entry == nil {
				break
			}

			// Decode command if present
			var cmd *Command
			if len(entry.Command) > 0 {
				cmd, _ = DecodeCommand(entry.Command)
			}

			toApply = append(toApply, ApplyMsg{
				CommandValid: true,
				Command:      cmd,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			})
		}
		rn.mu.Unlock()

		// Apply entries without holding lock
		for _, msg := range toApply {
			select {
			case rn.applyCh <- msg:
			case <-rn.stopCh:
				return
			}
		}

		// If nothing to apply, wait a bit
		if len(toApply) == 0 {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-rn.stopCh:
				return
			}
		}
	}
}

// Propose proposes a command to the Raft cluster.
// Returns the index and term of the proposed entry.
// The caller should wait for the entry to appear on ApplyCh.
func (rn *RaftNode) Propose(command []byte) (index int64, term int64, err error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return 0, 0, &ErrNotLeaderWithHint{
			LeaderID:   rn.leaderId,
			LeaderAddr: rn.peerAddrs[rn.leaderId],
		}
	}

	entry := LogEntry{
		Term:    rn.currentTerm,
		Index:   rn.log.NextIndex(),
		Command: command,
	}

	if err := rn.log.Append(entry); err != nil {
		return 0, 0, err
	}

	// Trigger immediate replication
	rn.triggerReplication()

	return entry.Index, entry.Term, nil
}

// ProposeCommand is a convenience method that encodes and proposes a Command.
func (rn *RaftNode) ProposeCommand(cmd *Command) (index int64, term int64, err error) {
	data, err := cmd.Encode()
	if err != nil {
		return 0, 0, err
	}
	return rn.Propose(data)
}
