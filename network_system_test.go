package main

import "testing"

func TestVniToEsi(t *testing.T) {
	esi := NewSystemNetworkStrategy().vniToEsi(100)
	result := "00:00:00:00:00:00:00:00:00:64"
	if esi != result {
		t.Errorf("VniToEsi(100) = %s; want %s", esi, result)
	}
}
