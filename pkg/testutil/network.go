package testutil

import (
	"net"
)

// GetFreeAddr returns a free local address for testing.
//
// WARNING: This function has a TOCTOU (time-of-check-time-of-use) race condition.
// The port is released after being discovered, so another process could claim it
// before the caller binds to it. For tests where this matters, use GetFreeListener
// instead, which keeps the port reserved until the caller is ready.
//
// This function is kept for backward compatibility with existing tests where the
// race window is acceptable (most unit tests run in isolation).
func GetFreeAddr() string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// GetFreeAddrs returns n free local addresses for testing.
//
// WARNING: In addition to the TOCTOU race from GetFreeAddr, there is a risk that
// the same port may appear multiple times in the returned slice if a released port
// is reassigned before the loop completes. For scenarios requiring guaranteed unique
// addresses, use GetFreeListeners instead.
func GetFreeAddrs(n int) []string {
	if n < 0 {
		panic("n must be non-negative")
	}
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = GetFreeAddr()
	}
	return addrs
}

// GetFreeListener returns a listener bound to a free local port.
// The caller is responsible for closing the listener.
// This avoids the TOCTOU race condition in GetFreeAddr by keeping the port reserved.
func GetFreeListener() net.Listener {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	return lis
}

// GetFreeListeners returns n listeners bound to free local ports.
// The caller is responsible for closing all listeners.
// This avoids the TOCTOU race condition in GetFreeAddrs by keeping ports reserved.
func GetFreeListeners(n int) []net.Listener {
	if n < 0 {
		panic("n must be non-negative")
	}
	listeners := make([]net.Listener, n)
	// Clean up any created listeners if we panic mid-loop (e.g., port exhaustion)
	defer func() {
		if r := recover(); r != nil {
			for _, lis := range listeners {
				if lis != nil {
					lis.Close()
				}
			}
			panic(r)
		}
	}()
	for i := 0; i < n; i++ {
		listeners[i] = GetFreeListener()
	}
	return listeners
}

// ListenerAddr returns the address string from a listener.
// Convenience function for use with GetFreeListener/GetFreeListeners.
func ListenerAddr(lis net.Listener) string {
	return lis.Addr().String()
}
