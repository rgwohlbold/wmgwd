package main

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func WipeDatabase(t *testing.T) {
	db, err := NewDatabase(context.Background(), Configuration{})
	if err != nil {
		t.Errorf("NewDatabase(config) = %v; want nil", err)
	}
	_, err = db.client.Delete(context.Background(), "*", clientv3.WithFromKey())
	if err != nil {
		t.Errorf("db.client.Delete(context.Background(), \"*\", clientv3.WithFromKey()) = %v; want nil", err)
	}
}

func AssertNetworkStrategy(t *testing.T, ns *MockNetworkStrategy, vni uint64, evpnAdvertised, ospfAdvertised, arpEnabled bool, gratuitousArp int) {
	if ns.evpnAdvertised[vni] != evpnAdvertised {
		t.Errorf("ns.evpnAdvertised[%v] = %v; want %v", vni, ns.evpnAdvertised[vni], evpnAdvertised)
	}
	if ns.ospfAdvertised[vni] != ospfAdvertised {
		t.Errorf("ns.ospfAdvertised[%v] = %v; want %v", vni, ns.ospfAdvertised[vni], ospfAdvertised)
	}
	if ns.arpEnabled[vni] != arpEnabled {
		t.Errorf("ns.arpEnabled[%v] = %v; want %v", vni, ns.arpEnabled[vni], arpEnabled)
	}
	if ns.gratuitousArp[vni] != gratuitousArp {
		t.Errorf("ns.gratuitousArp[%v] = %v; want %v", vni, ns.gratuitousArp[vni], gratuitousArp)
	}
}

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func RunTestDaemon(t *testing.T, wg *sync.WaitGroup, as AssignmentStrategy, afterVniEvent func(*Daemon, LeaderState, VniEvent) Verdict) {
	ns := NewMockNetworkStrategy()
	config := Configuration{
		Node:             "node-test-" + RandStringRunes(10),
		ScanInterval:     1 * time.Second,
		Vnis:             []uint64{100},
		MigrationTimeout: 0,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	daemon := NewDaemon(config, ns, as)
	daemon.eventProcessor = EventProcessorWrapper{
		cancel:         cancel,
		eventProcessor: daemon.eventProcessor,
		afterVniEvent:  afterVniEvent,
	}
	go func() {
		defer cancel()
		err := daemon.Run(ctx)
		if err != nil {
			t.Errorf("daemon.Run(ctx) = %v; want nil", err)
		}
		wg.Done()
	}()

}

func TestSingleDaemonFailover(t *testing.T) {
	WipeDatabase(t)
	assertHit := false
	var wg sync.WaitGroup
	wg.Add(1)
	RunTestDaemon(t, &wg, AssignSelf{}, func(d *Daemon, s LeaderState, e VniEvent) Verdict {
		if e.State.Type == Idle {
			assertHit = true
			AssertNetworkStrategy(t, d.networkStrategy.(*MockNetworkStrategy), 100, true, true, true, 1)
			db, err := NewDatabase(context.Background(), d.Config)
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
			if state.Current != d.Config.Node {
				t.Errorf("state.Current = %v; want %v", state.Current, d.Config.Node)
			}
			return VerdictStop
		}
		return VerdictContinue
	})
	wg.Wait()
	if !assertHit {
		t.Errorf("assertHit = %v; want true", assertHit)
	}
}

func TestTwoDaemonFailover(t *testing.T) {
	WipeDatabase(t)
	var lock sync.Mutex
	crashed := false
	recovered := false

	afterVniFunc := func(d *Daemon, s LeaderState, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		// Crash non-leader idle process
		if e.State.Type == Idle && e.State.Current == d.Config.Node && s.Node != d.Config.Node {
			AssertNetworkStrategy(t, d.networkStrategy.(*MockNetworkStrategy), 100, true, true, true, 1)
			crashed = true
			return VerdictStop
		} else if e.State.Type == Idle && e.State.Current == d.Config.Node && s.Node == d.Config.Node && crashed {
			AssertNetworkStrategy(t, d.networkStrategy.(*MockNetworkStrategy), 100, true, true, true, 1)
			recovered = true
		}
		return VerdictContinue
	}
	var wg sync.WaitGroup
	wg.Add(2)
	RunTestDaemon(t, &wg, AssignOther{}, afterVniFunc)
	RunTestDaemon(t, &wg, AssignOther{}, afterVniFunc)
	wg.Wait()
	if !recovered {
		t.Errorf("recovered = %v; want true", recovered)
	}
}

// TestCrashFailoverDecided tests that after a node crashes when it is assigned in FailoverDecided, the VNI will fail over to the next node.
func TestCrashFailoverDecided(t *testing.T) {
	WipeDatabase(t)
	var lock sync.Mutex
	numCrashed := 0
	failoverToLeader := false
	idleOnLeader := false

	afterVniFunc := func(d *Daemon, s LeaderState, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		// First two non-leader processes that are set to FailoverDecided crash
		if e.State.Type == FailoverDecided && e.State.Next != d.Config.Node && s.Node != d.Config.Node {
			// First two processes that are set to FailoverDecided crash
			if numCrashed < 2 {
				numCrashed++
				return VerdictStop
			}
			failoverToLeader = true
			return VerdictContinue
		} else if e.State.Type == Idle && e.State.Current == d.Config.Node {
			if failoverToLeader {
				idleOnLeader = true
			}
			return VerdictStop
		}
		return VerdictContinue
	}
	var wg sync.WaitGroup
	wg.Add(3)
	RunTestDaemon(t, &wg, AssignOther{}, afterVniFunc)
	RunTestDaemon(t, &wg, AssignOther{}, afterVniFunc)
	RunTestDaemon(t, &wg, AssignOther{}, afterVniFunc)
	wg.Wait()
	if !idleOnLeader {
		t.Errorf("idleOnLeader = %v; want true", idleOnLeader)
	}
}

func TestMain(m *testing.M) {
	cmd := exec.Command("/home/richard/progs/etcd/bin/etcd")
	err := cmd.Start()
	if err != nil {
		fmt.Printf("Error starting etcd: %v\n", err)
		os.Exit(1)
	}
	time.Sleep(2 * time.Second)
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