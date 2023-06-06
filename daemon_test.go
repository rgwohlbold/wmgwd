package main

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"math/rand"
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
	defer db.Close()
	_, err = db.client.Delete(context.Background(), "", clientv3.WithPrefix())
	if err != nil {
		t.Errorf("db.client.Delete(ctx, \"\", clientv3.WithPrefix()) = %v; want nil", err)
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
	if ns.gratuitousArp[vni] < gratuitousArp {
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

type TestDaemon struct {
	daemon *Daemon
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTestDaemon(as AssignmentStrategy, beforeVniEvent func(*Daemon, LeaderState, VniEvent) Verdict, afterVniEvent func(*Daemon, LeaderState, VniEvent) Verdict) TestDaemon {
	config := Configuration{
		Node:             RandStringRunes(3),
		ScanInterval:     1 * time.Second,
		Vnis:             []uint64{100},
		MigrationTimeout: 0,
	}
	newAs := as
	switch as.(type) {
	case AssignSelf:
		newAs = AssignSelf{Config: &config}
	case AssignOther:
		newAs = AssignOther{Config: &config}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	daemon := NewDaemon(config, NewMockNetworkStrategy(), newAs)
	daemon.eventProcessor = EventProcessorWrapper{
		daemon:         daemon,
		cancel:         cancel,
		eventProcessor: daemon.eventProcessor,
		beforeVniEvent: beforeVniEvent,
		afterVniEvent:  afterVniEvent,
	}
	return TestDaemon{
		daemon: daemon,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (d TestDaemon) Run(t *testing.T, wg *sync.WaitGroup) {
	go func() {
		defer d.cancel()
		err := d.daemon.Run(d.ctx)
		if err != nil {
			t.Errorf("daemon.Run(ctx) = %v; want nil", err)
		}
		wg.Done()
	}()
}

// TestSingleDaemon tests that the leader assigns itself to unassigned VNIs on startup
func TestSingleDaemonFailover(t *testing.T) {
	WipeDatabase(t)
	assertHit := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	NewTestDaemon(AssignConsistentHashing{}, NoopVniEvent, func(d *Daemon, s LeaderState, e VniEvent) Verdict {
		if e.State.Type == Idle {
			assertHit = true
			AssertNetworkStrategy(t, d.networkStrategy.(*MockNetworkStrategy), 100, true, true, true, 1)
			db, err := NewDatabase(d.Config)
			if err != nil {
				t.Errorf("NewDatabase(config) = %v; want nil", err)
			}
			defer db.Close()
			state, err := db.GetState(100, -1)
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
	}).Run(t, &wg)
	wg.Wait()
	if !assertHit {
		t.Errorf("assertHit = %v; want true", assertHit)
	}
}

// TestTwoDaemonFailover tests that when a non-leader crashes in Idle, the leader takes over
func TestTwoDaemonFailover(t *testing.T) {
	WipeDatabase(t)
	var lock sync.Mutex
	crashed := false
	recovered := false

	afterVniFunc := func(d *Daemon, leader LeaderState, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if !crashed && e.State.Type == Idle && e.State.Current == d.Config.Node && leader.Node != d.Config.Node {
			AssertNetworkStrategy(t, d.networkStrategy.(*MockNetworkStrategy), 100, true, true, true, 1)
			crashed = true
			return VerdictStop
		} else if crashed && e.State.Type == Idle && e.State.Current == d.Config.Node && leader.Node == d.Config.Node {
			AssertNetworkStrategy(t, d.networkStrategy.(*MockNetworkStrategy), 100, true, true, true, 1)
			recovered = true
			return VerdictStop
		}
		return VerdictContinue
	}
	var wg sync.WaitGroup
	wg.Add(2)
	NewTestDaemon(AssignOther{}, NoopVniEvent, afterVniFunc).Run(t, &wg)
	NewTestDaemon(AssignOther{}, NoopVniEvent, afterVniFunc).Run(t, &wg)
	wg.Wait()
	if !recovered {
		t.Errorf("recovered = %v; want true", recovered)
	}
}

// TestMigration tests that after the leader decides to migrate, the next node will reach Idle
func TestMigration(t *testing.T) {
	WipeDatabase(t)
	leaderAssignedItself := false
	migrationDecided := false
	idleReached := false
	leaderStopped := false
	var lock sync.Mutex
	var wg sync.WaitGroup
	afterVniFunc := func(d *Daemon, leader LeaderState, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if !leaderAssignedItself && e.State.Type == Idle && e.State.Current == d.Config.Node && leader.Node == d.Config.Node {
			leaderAssignedItself = true
			wg.Done()
			return VerdictContinue
		} else if leaderAssignedItself && !migrationDecided && e.State.Type == MigrationDecided && e.State.Next == d.Config.Node && leader.Node != d.Config.Node {
			// Non-leader node was assigned a migration
			migrationDecided = true
			return VerdictContinue
		} else if migrationDecided && !idleReached && e.State.Type == Idle && e.State.Current == d.Config.Node && leader.Node != d.Config.Node {
			// Non-leader node was successfully migrated
			idleReached = true
			return VerdictStop
		} else if idleReached {
			// Stop other node
			leaderStopped = true
			return VerdictStop
		}
		return VerdictContinue
	}
	wg.Add(1)
	NewTestDaemon(AssignOther{}, NoopVniEvent, afterVniFunc).Run(t, &wg)
	wg.Wait()
	wg.Add(2)
	NewTestDaemon(AssignOther{}, NoopVniEvent, afterVniFunc).Run(t, &wg)
	wg.Wait()
	if !leaderStopped {
		t.Errorf("leaderStopped = %v; want true", leaderStopped)
	}
}

// TestCrashFailoverDecided tests that after a node crashes when it is assigned in FailoverDecided, the VNI will fail over to the next node.
func TestCrashFailoverDecided(t *testing.T) {
	WipeDatabase(t)
	var lock sync.Mutex
	firstIdleCrashed := false
	secondFailoverCrashed := false
	thirdReachesIdle := false

	beforeVniFunc := func(d *Daemon, s LeaderState, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()

		if firstIdleCrashed && !secondFailoverCrashed && e.State.Type == FailoverDecided && e.State.Next == d.Config.Node && s.Node != d.Config.Node {
			// Second non-leader process crashes in FailoverDecided
			secondFailoverCrashed = true
			return VerdictStop
		}
		return VerdictContinue
	}

	afterVniFunc := func(d *Daemon, s LeaderState, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if !firstIdleCrashed && e.State.Type == Idle && e.State.Current == d.Config.Node && s.Node != d.Config.Node {
			// First non-leader idle process crashes
			firstIdleCrashed = true
			return VerdictStop
		} else if secondFailoverCrashed && e.State.Type == Idle && e.State.Current == d.Config.Node && s.Node == d.Config.Node {
			// Third process (leader) is assigned and reaches idle
			thirdReachesIdle = true
			return VerdictStop
		}
		return VerdictContinue
	}
	var wg sync.WaitGroup
	wg.Add(3)
	NewTestDaemon(AssignOther{}, beforeVniFunc, afterVniFunc).Run(t, &wg)
	NewTestDaemon(AssignOther{}, beforeVniFunc, afterVniFunc).Run(t, &wg)
	NewTestDaemon(AssignOther{}, beforeVniFunc, afterVniFunc).Run(t, &wg)
	wg.Wait()
	if !thirdReachesIdle {
		t.Errorf("idleOnLeader = %v; want true", thirdReachesIdle)
	}
}

type CrashType int

const (
	CrashCurrent CrashType = iota
	CrashNext
)

// SubTestCrashMigrationDecided tests that after a node crashes when it is assigned as current or next (depending on crashType) in state stateType, the VNI will fail over to the next node.
func SubTestCrashMigrationDecided(t *testing.T, stateType VniStateType, crashType CrashType) {
	WipeDatabase(t)
	var lock sync.Mutex
	var wg sync.WaitGroup
	leaderAssignedItself := false
	firstMigrationCrashed := false
	secondReachesIdle := false
	beforeVniFunc := func(d *Daemon, s LeaderState, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if leaderAssignedItself && !firstMigrationCrashed &&
			e.State.Type == stateType &&
			(crashType == CrashNext && e.State.Next == d.Config.Node || crashType == CrashCurrent && e.State.Current == d.Config.Node) {
			log.Info().Msg("first migration crashed")
			firstMigrationCrashed = true
			return VerdictStop
		}
		return VerdictContinue
	}
	afterVniFunc := func(d *Daemon, s LeaderState, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if !leaderAssignedItself && e.State.Type == Idle && e.State.Current == d.Config.Node && s.Node == d.Config.Node {
			log.Info().Msg("leader assigned itself")
			leaderAssignedItself = true
			wg.Done()
			return VerdictContinue
		} else if firstMigrationCrashed && e.State.Type == Idle && e.State.Current == d.Config.Node && s.Node != d.Config.Node {
			log.Info().Msg("second reaches idle")
			secondReachesIdle = true
			return VerdictStop
		} else if secondReachesIdle {
			return VerdictStop
		}
		return VerdictContinue
	}
	wg.Add(1)
	NewTestDaemon(AssignOther{}, beforeVniFunc, afterVniFunc).Run(t, &wg)
	wg.Wait()
	wg.Add(3)
	NewTestDaemon(AssignOther{}, beforeVniFunc, afterVniFunc).Run(t, &wg)
	NewTestDaemon(AssignOther{}, beforeVniFunc, afterVniFunc).Run(t, &wg)
	wg.Wait()
	if !secondReachesIdle {
		t.Errorf("secondReachesIdle = %v; want true", secondReachesIdle)
	}
}

func TestCrashNextMigrationDecided(t *testing.T) {
	SubTestCrashMigrationDecided(t, MigrationDecided, CrashNext)
}

func TestCrashCurrentMigrationOspfAdvertised(t *testing.T) {
	SubTestCrashMigrationDecided(t, MigrationOspfAdvertised, CrashCurrent)
}

func TestCrashNextMigrationOspfWithdrawn(t *testing.T) {
	SubTestCrashMigrationDecided(t, MigrationOspfWithdrawn, CrashNext)
}

func TestCrashCurrentArpEnabled(t *testing.T) {
	SubTestCrashMigrationDecided(t, MigrationArpEnabled, CrashCurrent)
}

func TestCrashNextArpDisabled(t *testing.T) {
	SubTestCrashMigrationDecided(t, MigrationArpDisabled, CrashNext)
}

func TestCrashCurrentGratuitousArpSent(t *testing.T) {
	SubTestCrashMigrationDecided(t, MigrationGratuitousArpSent, CrashCurrent)
}

func TestCrashNextEvpnWithdrawn(t *testing.T) {
	SubTestCrashMigrationDecided(t, MigrationEvpnWithdrawn, CrashNext)
}

func TestDrainOnShutdown(t *testing.T) {
	WipeDatabase(t)
	var lock sync.Mutex
	var wg sync.WaitGroup
	leaderAssignedItself := false
	secondReachesMigrate := false
	afterVniFunc := func(d *Daemon, s LeaderState, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if !leaderAssignedItself && e.State.Type == Idle && e.State.Current == d.Config.Node && s.Node == d.Config.Node {
			log.Info().Str("node", d.Config.Node).Msg("leader assigned itself")
			leaderAssignedItself = true
			return VerdictStop
		} else if leaderAssignedItself && e.State.Type == MigrationDecided && e.State.Next == d.Config.Node && s.Node != d.Config.Node {
			log.Info().Str("node", d.Config.Node).Msg("second reaches MigrationDecided")
			secondReachesMigrate = true
			return VerdictStop
		}
		return VerdictContinue
	}
	wg.Add(2)
	testDaemon1 := NewTestDaemon(AssignSelf{}, NoopVniEvent, afterVniFunc)
	testDaemon1.daemon.Config.DrainOnShutdown = true
	testDaemon2 := NewTestDaemon(AssignSelf{}, NoopVniEvent, afterVniFunc)
	testDaemon2.daemon.Config.DrainOnShutdown = true
	testDaemon1.Run(t, &wg)
	testDaemon2.Run(t, &wg)
	wg.Wait()
	if !secondReachesMigrate {
		t.Errorf("secondReachesIdle = %v; want true", secondReachesMigrate)
	}

}

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	cmd := exec.Command("/home/richard/progs/etcd/bin/etcd")
	err := cmd.Start()
	if err != nil {
		fmt.Printf("Error starting etcd: %v\n", err)
		os.Exit(1)
	}
	db, err := NewDatabase(Configuration{Node: "db-starter"})
	if err != nil {
		fmt.Printf("Error creating database: %v\n", err)
		os.Exit(1)
	}
	db.Close()
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
