package raft

import (
	"context"

	"go.uber.org/zap"

	"github.com/yash1thredddy/Distributed-Consensus-KV-Store/api/proto/raftpb"
)

// --------------------------------------------------------------------------
// RPC Handler implementations
// Handles incoming RequestVote, AppendEntries, and InstallSnapshot RPCs
// --------------------------------------------------------------------------

// HandleRequestVote handles incoming RequestVote RPCs.
// Implements transport.VoteHandler interface.
func (rn *RaftNode) HandleRequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &raftpb.RequestVoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	// Rule 1: Reply false if term < currentTerm
	if req.Term < rn.currentTerm {
		return resp, nil
	}

	// If request term > currentTerm, update term and step down
	if req.Term > rn.currentTerm {
		rn.stepDown(req.Term)
	}

	resp.Term = rn.currentTerm

	// Check if we can grant vote
	// We can vote if:
	// 1. We haven't voted for anyone in this term, or we already voted for this candidate
	// 2. Candidate's log is at least as up-to-date as ours
	canVote := rn.votedFor == "" || rn.votedFor == req.CandidateId
	logUpToDate := rn.isLogUpToDate(req.LastLogIndex, req.LastLogTerm)

	if canVote && logUpToDate {
		// CRITICAL: Persist before responding
		// Save old value in case persistence fails
		oldVotedFor := rn.votedFor
		rn.votedFor = req.CandidateId

		if err := rn.persistState(); err != nil {
			rn.logger.Error("failed to persist vote",
				zap.String("candidateId", req.CandidateId),
				zap.Int64("term", req.Term),
				zap.Error(err))
			// Can't persist, restore old state and don't grant vote
			rn.votedFor = oldVotedFor
			return resp, nil
		}

		resp.VoteGranted = true
		rn.resetElectionTimer()
	}

	return resp, nil
}

// HandleAppendEntries handles incoming AppendEntries RPCs.
// Implements transport.EntriesHandler interface.
func (rn *RaftNode) HandleAppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &raftpb.AppendEntriesResponse{
		Term:    rn.currentTerm,
		Success: false,
	}

	// Rule 1: Reply false if term < currentTerm
	if req.Term < rn.currentTerm {
		return resp, nil
	}

	// If request term > currentTerm, update term
	if req.Term > rn.currentTerm {
		rn.stepDown(req.Term)
	}

	// Valid AppendEntries from current leader
	// Reset to follower (if candidate) and reset election timer
	if rn.state == Candidate {
		rn.state = Follower
	}
	rn.leaderId = req.LeaderId
	rn.resetElectionTimer()

	resp.Term = rn.currentTerm

	// Rule 2: Reply false if log doesn't contain an entry at prevLogIndex
	// with matching prevLogTerm
	if req.PrevLogIndex > 0 {
		if rn.log.LastIndex() < req.PrevLogIndex {
			// We don't have the entry at prevLogIndex
			resp.ConflictIndex = rn.log.LastIndex() + 1
			resp.ConflictTerm = 0
			return resp, nil
		}

		prevEntry := rn.log.GetEntry(req.PrevLogIndex)
		if prevEntry == nil || prevEntry.Term != req.PrevLogTerm {
			// Term mismatch - find conflict point
			if prevEntry != nil {
				resp.ConflictTerm = prevEntry.Term
				// Find first entry with conflicting term
				resp.ConflictIndex = rn.log.findFirstIndexOfTerm(resp.ConflictTerm)
			} else {
				resp.ConflictIndex = rn.log.LastIndex() + 1
				resp.ConflictTerm = 0
			}
			return resp, nil
		}
	}

	// Rule 3 & 4: Process entries
	if len(req.Entries) > 0 {
		entries := convertProtoToLogEntries(req.Entries)
		for _, entry := range entries {
			existing := rn.log.GetEntry(entry.Index)
			if existing != nil && existing.Term != entry.Term {
				// Conflict - truncate from here
				if err := rn.log.TruncateAfter(entry.Index - 1); err != nil {
					rn.logger.Error("failed to truncate log",
						zap.Int64("index", entry.Index-1),
						zap.Error(err))
					return resp, nil
				}
			}

			if existing == nil || existing.Term != entry.Term {
				// Append new entry
				if err := rn.log.Append(entry); err != nil {
					rn.logger.Error("failed to append log entry",
						zap.Int64("index", entry.Index),
						zap.Int64("term", entry.Term),
						zap.Error(err))
					return resp, nil
				}
			}
			// If terms match, entry is identical - skip
		}
	}

	// Rule 5: Update commitIndex
	if req.LeaderCommit > rn.commitIndex {
		newCommitIndex := req.LeaderCommit
		lastIndex := rn.log.LastIndex()
		if lastIndex < newCommitIndex {
			newCommitIndex = lastIndex
		}
		rn.commitIndex = newCommitIndex
	}

	resp.Success = true
	resp.MatchIndex = rn.log.LastIndex()
	return resp, nil
}

// HandleInstallSnapshot handles incoming InstallSnapshot RPCs.
// Implements transport.SnapshotHandler interface.
func (rn *RaftNode) HandleInstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &raftpb.InstallSnapshotResponse{
		Term: rn.currentTerm,
	}

	// Reply immediately if term < currentTerm
	if req.Term < rn.currentTerm {
		return resp, nil
	}

	// If term > currentTerm, update term
	if req.Term > rn.currentTerm {
		rn.stepDown(req.Term)
		resp.Term = rn.currentTerm
	}

	// Save snapshot first - don't update any state until storage succeeds
	if err := rn.storage.SaveSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data); err != nil {
		rn.logger.Error("failed to save snapshot",
			zap.Int64("lastIncludedIndex", req.LastIncludedIndex),
			zap.Int64("lastIncludedTerm", req.LastIncludedTerm),
			zap.Error(err))
		return resp, nil
	}

	// Truncate log up to snapshot point
	if err := rn.storage.TruncateLogBefore(req.LastIncludedIndex + 1); err != nil {
		rn.logger.Error("failed to truncate log before snapshot",
			zap.Int64("index", req.LastIncludedIndex+1),
			zap.Error(err))
		// Log truncation failed, but snapshot is saved
		// This is a partial failure state, but we can recover on restart
		return resp, nil
	}

	// Storage operations succeeded, now update leader info
	rn.leaderId = req.LeaderId
	rn.resetElectionTimer()

	// Send snapshot to state machine - must succeed before updating indices
	// Use blocking send with context cancellation support
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotTerm:  req.LastIncludedTerm,
		SnapshotIndex: req.LastIncludedIndex,
	}

	select {
	case rn.applyCh <- msg:
		// Successfully sent to state machine, now update indices
		if req.LastIncludedIndex > rn.commitIndex {
			rn.commitIndex = req.LastIncludedIndex
		}
		if req.LastIncludedIndex > rn.lastApplied {
			rn.lastApplied = req.LastIncludedIndex
		}
	case <-ctx.Done():
		// Context cancelled, don't update indices
		return resp, ctx.Err()
	}

	return resp, nil
}
