package main

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func WipeDatabase(t *testing.T) {
	db, err := NewDatabase(Configuration{})
	if err != nil {
		t.Errorf("NewDatabase(config) = %v; want nil", err)
	}
	_, err = db.client.Delete(context.Background(), "*", clientv3.WithFromKey())
	if err != nil {
		t.Errorf("db.client.Delete(context.Background(), \"*\", clientv3.WithFromKey()) = %v; want nil", err)
	}
}

func TestSingleDaemonFailover(t *testing.T) {
	WipeDatabase(t)
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

func TestTwoDaemonFailover(t *testing.T) {
	WipeDatabase(t)
	ns1 := NewMockNetworkStrategy()
	ns2 := NewMockNetworkStrategy()
	config1 := Configuration{
		Node:             "test-two-daemon-1",
		Vnis:             []uint64{100},
		MigrationTimeout: 0,
	}
	config2 := Configuration{
		Node:             "test-two-daemon-2",
		Vnis:             []uint64{100},
		MigrationTimeout: 1 * time.Millisecond,
	}
	daemon1 := NewDaemon(config1, ns1)
	daemon2 := NewDaemon(config2, ns2)
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	wg1 := sync.WaitGroup{}
	wg1.Add(1)
	go func() {
		err := daemon1.Run(ctx1)
		if err != nil {
			t.Errorf("daemon1.Run(ctx1) = %v; want nil", err)
		}
		wg1.Done()
	}()

	time.Sleep(1 * time.Second)

	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		err := daemon2.Run(ctx2)
		if err != nil {
			t.Errorf("daemon2.Run(ctx2) = %v; want nil", err)
		}
		wg2.Done()
	}()
	time.Sleep(1 * time.Second)
	if ns1.numEvpnAdvertised[100] != 1 {
		t.Errorf("ns1.numEvpnAdvertised[100] = %v; want 1", ns1.numEvpnAdvertised[100])
	}
	if ns1.numOspfAdvertised[100] != 1 {
		t.Errorf("ns1.numOspfAdvertised[100] = %v; want 1", ns1.numOspfAdvertised[100])
	}
	if ns1.numArpEnabled[100] != 1 {
		t.Errorf("ns1.numArpEnabled[100] = %v; want 1", ns1.numArpEnabled[100])
	}
	if ns1.numGratuitousArp[100] != 1 {
		t.Errorf("ns1.numGratuitousArp[100] = %v; want 1", ns1.numGratuitousArp[100])
	}
	if ns2.numEvpnAdvertised[100] != 0 {
		t.Errorf("ns2.numEvpnAdvertised[100] = %v; want 0", ns1.numEvpnAdvertised[100])
	}
	if ns2.numOspfAdvertised[100] != 0 {
		t.Errorf("ns2.numOspfAdvertised[100] = %v; want 0", ns1.numOspfAdvertised[100])
	}
	if ns2.numArpEnabled[100] != 0 {
		t.Errorf("ns2.numArpEnabled[100] = %v; want 0", ns1.numArpEnabled[100])
	}
	if ns2.numGratuitousArp[100] != 0 {
		t.Errorf("ns2.numGratuitousArp[100] = %v; want 0", ns1.numGratuitousArp[100])
	}
	cancel1()
	wg1.Wait()
	time.Sleep(10 * time.Millisecond)
	if ns2.numEvpnAdvertised[100] != 1 {
		t.Errorf("ns2.numEvpnAdvertised[100] = %v; want 1", ns2.numEvpnAdvertised[100])
	}
	if ns2.numOspfAdvertised[100] != 1 {
		t.Errorf("ns2.numOspfAdvertised[100] = %v; want 1", ns2.numOspfAdvertised[100])
	}
	if ns2.numArpEnabled[100] != 1 {
		t.Errorf("ns2.numArpEnabled[100] = %v; want 1", ns2.numArpEnabled[100])
	}
	if ns2.numGratuitousArp[100] != 1 {
		t.Errorf("ns2.numGratuitousArp[100] = %v; want 1", ns2.numGratuitousArp[100])
	}
	cancel2()
	wg2.Wait()
}

func TestMain(m *testing.M) {
	cmd := exec.Command("/home/richard/progs/etcd/bin/etcd")
	err := cmd.Start()
	if err != nil {
		fmt.Printf("Error starting etcd: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	err = cmd.Process.Kill()
	if err != nil {
		fmt.Printf("Error killing etcd: %v\n", err)
		os.Exit(1)
	}
	err = os.RemoveAll("default.etcd")
	if err != nil {
		fmt.Printf("Error removing etcd data: %v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}
