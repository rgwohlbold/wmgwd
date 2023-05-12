package main

import (
	"context"
	"testing"
	"time"
)

func TestSingleDaemonFailover(t *testing.T) {
	networkStrategy := NewMockNetworkStrategy()
	config := Configuration{
		Node:             "test-single-daemon",
		Vnis:             []uint64{100},
		MigrationTimeout: 0,
	}
	daemon := NewDaemon(config, networkStrategy)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := daemon.Run(ctx)
	if err != nil {
		t.Errorf("daemon.Run(ctx) = %v; want nil", err)
	}
	if networkStrategy.numEvpnAdvertised[100] != 1 {
		t.Errorf("networkStrategy.numEvpnAdvertised[100] = %v; want 1", networkStrategy.numEvpnAdvertised[100])
	}
	if networkStrategy.numOspfAdvertised[100] != 1 {
		t.Errorf("networkStrategy.numOspfAdvertised[100] = %v; want 1", networkStrategy.numOspfAdvertised[100])
	}
	if networkStrategy.numArpEnabled[100] != 1 {
		t.Errorf("networkStrategy.numArpEnabled[100] = %v; want 1", networkStrategy.numArpEnabled[100])
	}
	if networkStrategy.numGratuitousArp[100] != 1 {
		t.Errorf("networkStrategy.numGratuitousArp[100] = %v; want 1", networkStrategy.numGratuitousArp[100])
	}
	db, err := NewDatabase(config)
	if err != nil {
		t.Errorf("NewDatabase(config) = %v; want nil", err)
	}
	state, err := db.GetState(100)
	if err != nil {
		t.Errorf("db.GetState(100) = %v; want nil", err)
	}
	if state.Type != Idle {
		t.Errorf("state.Type = %v; want Idle", state.Type)
	}
	if state.Current != config.Node {
		t.Errorf("state.Current = %v; want %v", state.Current, config.Node)
	}
}
