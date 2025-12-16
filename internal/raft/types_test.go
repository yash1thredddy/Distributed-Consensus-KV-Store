package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{Follower, "Follower"},
		{Candidate, "Candidate"},
		{Leader, "Leader"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestCommandTypeString(t *testing.T) {
	tests := []struct {
		cmdType  CommandType
		expected string
	}{
		{CommandPut, "Put"},
		{CommandDelete, "Delete"},
		{CommandNoop, "Noop"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.cmdType.String())
		})
	}
}

func TestCommandEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cmd  *Command
	}{
		{
			name: "Put command",
			cmd: &Command{
				Type:  CommandPut,
				Key:   "test-key",
				Value: []byte("test-value"),
			},
		},
		{
			name: "Delete command",
			cmd: &Command{
				Type: CommandDelete,
				Key:  "test-key",
			},
		},
		{
			name: "Noop command",
			cmd: &Command{
				Type: CommandNoop,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			data, err := tt.cmd.Encode()
			require.NoError(t, err)
			require.NotNil(t, data)

			// Decode
			decoded, err := DecodeCommand(data)
			require.NoError(t, err)
			require.NotNil(t, decoded)

			// Verify
			assert.Equal(t, tt.cmd.Type, decoded.Type)
			assert.Equal(t, tt.cmd.Key, decoded.Key)
			assert.Equal(t, tt.cmd.Value, decoded.Value)
		})
	}
}

func TestCommandEncodeDecodeError(t *testing.T) {
	// Test decode with invalid data
	_, err := DecodeCommand([]byte("invalid json"))
	assert.Error(t, err)
}

func TestPersistentState(t *testing.T) {
	state := &PersistentState{
		CurrentTerm: 5,
		VotedFor:    "node1",
	}

	assert.Equal(t, int64(5), state.CurrentTerm)
	assert.Equal(t, "node1", state.VotedFor)
}

func TestLogEntry(t *testing.T) {
	cmd := &Command{
		Type:  CommandPut,
		Key:   "key1",
		Value: []byte("value1"),
	}

	cmdData, err := cmd.Encode()
	require.NoError(t, err)

	entry := &LogEntry{
		Term:    1,
		Index:   1,
		Command: cmdData,
	}

	assert.Equal(t, int64(1), entry.Term)
	assert.Equal(t, int64(1), entry.Index)
	assert.NotNil(t, entry.Command)
}

func TestApplyMsg(t *testing.T) {
	t.Run("Command apply message", func(t *testing.T) {
		cmd := &Command{
			Type:  CommandPut,
			Key:   "key1",
			Value: []byte("value1"),
		}

		msg := &ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: 1,
			CommandTerm:  1,
		}

		assert.True(t, msg.CommandValid)
		assert.False(t, msg.SnapshotValid)
		assert.Equal(t, int64(1), msg.CommandIndex)
		assert.Equal(t, int64(1), msg.CommandTerm)
	})

	t.Run("Snapshot apply message", func(t *testing.T) {
		msg := &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      []byte("snapshot-data"),
			SnapshotTerm:  5,
			SnapshotIndex: 100,
		}

		assert.True(t, msg.SnapshotValid)
		assert.False(t, msg.CommandValid)
		assert.Equal(t, int64(5), msg.SnapshotTerm)
		assert.Equal(t, int64(100), msg.SnapshotIndex)
	})
}
