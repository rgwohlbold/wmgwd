package main

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"math"
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

func NewTestDaemon(config Configuration, as AssignmentStrategy, beforeVniEvent func(*Daemon, VniEvent) Verdict, afterVniEvent func(*Daemon, VniEvent) Verdict) TestDaemon {
	if config.Node == "" {
		config.Node = RandStringRunes(3)
	}
	config.ScanInterval = 1 * time.Second
	if config.Vnis == nil {
		config.Vnis = []uint64{100}
	}
	config.MigrationTimeout = 0
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

// TestSingleDaemon tests that a single daemon assigns itself to unassigned VNIs on startup
func TestSingleDaemonFailover(t *testing.T) {
	WipeDatabase(t)
	assertHit := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	NewTestDaemon(Configuration{}, AssignConsistentHashing{}, NoopVniEvent, func(d *Daemon, e VniEvent) Verdict {
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

// TestTwoDaemonFailover tests that when a one node crashes in Idle, the leader takes over
func TestTwoDaemonFailover(t *testing.T) {
	WipeDatabase(t)
	var lock sync.Mutex
	crashed := false
	recovered := false
	var originalNode string

	afterVniFunc := func(d *Daemon, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if !crashed && e.State.Type == Idle && e.State.Current == d.Config.Node {
			originalNode = d.Config.Node
			AssertNetworkStrategy(t, d.networkStrategy.(*MockNetworkStrategy), 100, true, true, true, 1)
			crashed = true
			return VerdictStop
		} else if crashed && e.State.Type == Idle && e.State.Current == d.Config.Node && originalNode != d.Config.Node {
			AssertNetworkStrategy(t, d.networkStrategy.(*MockNetworkStrategy), 100, true, true, true, 1)
			recovered = true
			return VerdictStop
		}
		return VerdictContinue
	}
	var wg sync.WaitGroup
	wg.Add(2)
	NewTestDaemon(Configuration{}, AssignOther{}, NoopVniEvent, afterVniFunc).Run(t, &wg)
	NewTestDaemon(Configuration{}, AssignOther{}, NoopVniEvent, afterVniFunc).Run(t, &wg)
	wg.Wait()
	if !recovered {
		t.Errorf("recovered = %v; want true", recovered)
	}
}

// TestMigration tests that after a migration the next node will reach Idle
func TestMigration(t *testing.T) {
	WipeDatabase(t)
	nodeAssignedItself := false
	var firstNode string
	var secondNode string
	migrationDecided := false
	idleReached := false
	leaderStopped := false
	var lock sync.Mutex
	var wg sync.WaitGroup
	afterVniFunc := func(d *Daemon, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if !nodeAssignedItself && e.State.Type == Idle && e.State.Current == d.Config.Node {
			firstNode = d.Config.Node
			nodeAssignedItself = true
			wg.Done()
			return VerdictContinue
		} else if nodeAssignedItself && !migrationDecided && e.State.Type == MigrationDecided && e.State.Next == d.Config.Node && firstNode != d.Config.Node {
			// Other node was assigned a migration
			migrationDecided = true
			secondNode = d.Config.Node
			return VerdictContinue
		} else if migrationDecided && !idleReached && e.State.Type == Idle && e.State.Current == d.Config.Node && d.Config.Node == secondNode {
			// Node was successfully migrated
			idleReached = true
			return VerdictStop
		} else if idleReached {
			// Stop other node
			leaderStopped = true
			return VerdictStop
		}
		return VerdictContinue
	}
	vnis := []uint64{FindVniThatMapsBetween(1, math.MaxUint64)}
	wg.Add(1)
	daemon1 := NewTestDaemon(Configuration{Vnis: vnis}, AssignConsistentHashing{}, NoopVniEvent, afterVniFunc)
	daemon1.daemon.uids = []uint64{math.MaxUint64}
	daemon1.Run(t, &wg)
	wg.Wait()
	wg.Add(2)
	daemon2 := NewTestDaemon(Configuration{Vnis: vnis}, AssignConsistentHashing{}, NoopVniEvent, afterVniFunc)
	daemon2.daemon.uids = []uint64{math.MaxUint64 - 1}
	daemon2.Run(t, &wg)
	wg.Wait()
	if !leaderStopped {
		t.Errorf("leaderStopped = %v; want true", leaderStopped)
	}
}

// TestCrashFailoverDecided tests that after a node crashes when it is assigned in FailoverDecided, the VNI will fail over to the next node.
func TestCrashFailoverDecided(t *testing.T) {
	WipeDatabase(t)
	var lock sync.Mutex
	var firstNode string
	var secondNode string
	firstIdleCrashed := false
	secondFailoverCrashed := false
	thirdReachesIdle := false

	beforeVniFunc := func(d *Daemon, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()

		if firstIdleCrashed && !secondFailoverCrashed && e.State.Type == FailoverDecided && e.State.Next == d.Config.Node && d.Config.Node != firstNode {
			// Crash in FailoverDecided
			secondFailoverCrashed = true
			secondNode = d.Config.Node
			return VerdictStop
		}
		return VerdictContinue
	}

	afterVniFunc := func(d *Daemon, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if !firstIdleCrashed && e.State.Type == Idle && e.State.Current == d.Config.Node {
			// First idle process crashes
			firstIdleCrashed = true
			firstNode = d.Config.Node
			return VerdictStop
		} else if secondFailoverCrashed && e.State.Type == Idle && e.State.Current == d.Config.Node && d.Config.Node != firstNode && d.Config.Node != secondNode {
			// Third process reaches idle
			thirdReachesIdle = true
			return VerdictStop
		}
		return VerdictContinue
	}
	var wg sync.WaitGroup
	wg.Add(3)
	vnis := []uint64{FindVniThatMapsBetween(1, math.MaxUint64-1)}
	daemon1 := NewTestDaemon(Configuration{Vnis: vnis}, AssignConsistentHashing{}, beforeVniFunc, afterVniFunc)
	daemon1.daemon.uids = []uint64{math.MaxUint64}
	daemon1.Run(t, &wg)
	daemon2 := NewTestDaemon(Configuration{Vnis: vnis}, AssignConsistentHashing{}, beforeVniFunc, afterVniFunc)
	daemon2.daemon.uids = []uint64{math.MaxUint64 - 1}
	daemon2.Run(t, &wg)
	daemon3 := NewTestDaemon(Configuration{Vnis: vnis}, AssignConsistentHashing{}, beforeVniFunc, afterVniFunc)
	daemon3.daemon.uids = []uint64{math.MaxUint64 - 2}
	daemon3.Run(t, &wg)
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
	var firstNode string
	var crashingNode string
	secondReachesIdle := false
	beforeVniFunc := func(d *Daemon, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if firstNode != "" && crashingNode == "" &&
			e.State.Type == stateType &&
			(crashType == CrashNext && e.State.Next == d.Config.Node || crashType == CrashCurrent && e.State.Current == d.Config.Node) {
			crashingNode = d.Config.Node
			log.Info().Str("node", d.Config.Node).Msg("crashing")
			return VerdictStop
		}
		return VerdictContinue
	}
	afterVniFunc := func(d *Daemon, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if firstNode == "" && e.State.Type == Idle && e.State.Current == d.Config.Node {
			log.Info().Str("node", d.Config.Node).Msg("reached idle")
			firstNode = d.Config.Node
			wg.Done()
			return VerdictContinue
		} else if crashingNode != "" && e.State.Type == Idle && e.State.Current == d.Config.Node && d.Config.Node != crashingNode {
			log.Info().Str("node", d.Config.Node).Msg("reached idle")
			secondReachesIdle = true
			return VerdictStop
		} else if secondReachesIdle {
			return VerdictStop
		}
		return VerdictContinue
	}
	vnis := []uint64{FindVniThatMapsBetween(1, math.MaxUint64-1)}
	wg.Add(1)

	daemon1 := NewTestDaemon(Configuration{Vnis: vnis}, AssignConsistentHashing{}, beforeVniFunc, afterVniFunc)
	daemon1.daemon.uids = []uint64{math.MaxUint64}
	daemon1.Run(t, &wg)

	wg.Wait()

	daemon2 := NewTestDaemon(Configuration{Vnis: vnis}, AssignConsistentHashing{}, beforeVniFunc, afterVniFunc)
	daemon2.daemon.uids = []uint64{math.MaxUint64 - 1}
	daemon2.Run(t, &wg)

	wg.Add(2)
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
	firstNode := ""
	secondReachesMigrate := false
	afterVniFunc := func(d *Daemon, e VniEvent) Verdict {
		lock.Lock()
		defer lock.Unlock()
		if firstNode == "" && e.State.Type == Idle && e.State.Current == d.Config.Node {
			log.Info().Str("node", d.Config.Node).Msg("first node assigned itself")
			firstNode = d.Config.Node
			return VerdictStop
		} else if firstNode != "" && e.State.Type == MigrationDecided && e.State.Next == d.Config.Node && firstNode != d.Config.Node {
			log.Info().Str("node", d.Config.Node).Msg("second reaches MigrationDecided")
			secondReachesMigrate = true
			return VerdictStop
		}
		return VerdictContinue
	}
	wg.Add(2)
	NewTestDaemon(Configuration{DrainOnShutdown: true}, AssignSelf{}, NoopVniEvent, afterVniFunc).Run(t, &wg)
	NewTestDaemon(Configuration{DrainOnShutdown: true}, AssignSelf{}, NoopVniEvent, afterVniFunc).Run(t, &wg)
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
