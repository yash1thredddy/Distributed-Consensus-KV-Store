package testutil

import (
	"time"

	"github.com/yourusername/distributed-kv/internal/raft"
)

// WaitForCondition waits for a condition to be true within timeout.
// Returns true if condition was met, false if timeout occurred.
func WaitForCondition(fn func() bool, timeout time.Duration) bool {
	return WaitForConditionWithInterval(fn, timeout, 10*time.Millisecond)
}

// WaitForConditionWithInterval waits for a condition with custom poll interval.
func WaitForConditionWithInterval(fn func() bool, timeout, interval time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}

// WaitForApply waits for an entry with the given index to be applied.
// Returns the apply message and true if found, empty message and false on timeout.
func WaitForApply(applyCh <-chan raft.ApplyMsg, index int64, timeout time.Duration) (raft.ApplyMsg, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case msg := <-applyCh:
			if msg.CommandIndex == index {
				return msg, true
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	return raft.ApplyMsg{}, false
}

// WaitForApplyFunc waits for an apply message that satisfies the predicate.
func WaitForApplyFunc(applyCh <-chan raft.ApplyMsg, pred func(raft.ApplyMsg) bool, timeout time.Duration) (raft.ApplyMsg, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case msg := <-applyCh:
			if pred(msg) {
				return msg, true
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	return raft.ApplyMsg{}, false
}

// DrainChannel drains a channel without blocking.
func DrainChannel[T any](ch <-chan T) []T {
	var items []T
	for {
		select {
		case item := <-ch:
			items = append(items, item)
		default:
			return items
		}
	}
}

// Eventually retries a function until it returns nil error or timeout.
func Eventually(fn func() error, timeout time.Duration) error {
	return EventuallyWithInterval(fn, timeout, 10*time.Millisecond)
}

// EventuallyWithInterval retries with custom interval.
func EventuallyWithInterval(fn func() error, timeout, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(interval)
	}
	return lastErr
}
