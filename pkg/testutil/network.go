package testutil

import (
	"net"
)

// GetFreeAddr returns a free local address for testing.
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
func GetFreeAddrs(n int) []string {
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = GetFreeAddr()
	}
	return addrs
}
