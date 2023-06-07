package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type EventProcessor interface {
	Process(ctx context.Context, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent) error
}

type DefaultEventProcessor struct {
	daemon *Daemon
	leader *LeaderState

	isAssigning *bool
	willAssign  *bool
	assignMutex *sync.Mutex
}

type Verdict int

const (
	VerdictContinue Verdict = iota
	VerdictStop
)

type EventProcessorWrapper struct {
	daemon         *Daemon
	cancel         context.CancelFunc
	eventProcessor EventProcessor
	afterVniEvent  func(*Daemon, LeaderState, VniEvent) Verdict
	beforeVniEvent func(*Daemon, LeaderState, VniEvent) Verdict
}

func NoopVniEvent(_ *Daemon, _ LeaderState, _ VniEvent) Verdict {
	return VerdictContinue
}

func NewDefaultEventProcessor(daemon *Daemon) *DefaultEventProcessor {
	isAssigning := false
	willAssign := false
	return &DefaultEventProcessor{
		leader:      &LeaderState{},
		daemon:      daemon,
		isAssigning: &isAssigning,
		willAssign:  &willAssign,
		assignMutex: &sync.Mutex{},
	}
}

func (p DefaultEventProcessor) NewVniUpdate(vni uint64) *VniUpdate {
	return p.daemon.db.NewVniUpdate(vni)
}

func (p DefaultEventProcessor) ProcessVniEventSync(ctx context.Context, event VniEvent) error {
	state := event.State.Type
	current := event.State.Current
	next := event.State.Next
	isCurrent := current == p.daemon.Config.Node
	isNext := next == p.daemon.Config.Node
	isLeader := p.leader.Node == p.daemon.Config.Node

	// Failure detection and assignment
	if isLeader {
		if state == Unassigned {
			p.PeriodicAssignmentSync(ctx)
		} else {
			// All states except Unassigned and Idle need "Next"
			if next == "" && state != Idle {
				p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).RunWithRetry()
			}
			// All states except Unassigned, FailoverDecided need "Current"
			if current == "" && state != Unassigned && state != FailoverDecided {
				p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).RunWithRetry()
			}
		}
	}

	if state == MigrationDecided && isNext {
		err := p.daemon.networkStrategy.AdvertiseEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise evpn")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		err = p.daemon.networkStrategy.AdvertiseOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise ospf")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfAdvertised).RunWithRetry()
	} else if state == MigrationOspfAdvertised && isCurrent {
		err := p.daemon.networkStrategy.WithdrawOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw ospf")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfWithdrawn).RunWithRetry()
	} else if state == MigrationOspfWithdrawn && isNext {
		err := p.daemon.networkStrategy.EnableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not enable arp")
		}
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpEnabled).RunWithRetry()
	} else if state == MigrationArpEnabled && isCurrent {
		err := p.daemon.networkStrategy.DisableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not disable arp")
		}
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpDisabled).RunWithRetry()
	} else if state == MigrationArpDisabled && isNext {
		err := p.daemon.networkStrategy.SendGratuitousArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not send gratuitous arp")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationGratuitousArpSent).RunWithRetry()
	} else if state == MigrationGratuitousArpSent && isCurrent {
		err := p.daemon.networkStrategy.WithdrawEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw evpn")
		}
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationEvpnWithdrawn).RunWithRetry()
	} else if state == MigrationEvpnWithdrawn && isNext {
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).RunWithRetry()
	} else if state == FailoverDecided && isNext {
		err := p.daemon.networkStrategy.AdvertiseEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise evpn")
		}
		err = p.daemon.networkStrategy.AdvertiseOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise ospf")
		}
		err = p.daemon.networkStrategy.EnableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not enable arp")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		err = p.daemon.networkStrategy.SendGratuitousArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not send gratuitous arp")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).RunWithRetry()
	}
	return nil
}

func (p DefaultEventProcessor) ProcessVniEventAsync(ctx context.Context, event VniEvent) {
	go func() {
		err := p.ProcessVniEventSync(ctx, event)
		if err != nil {
			p.daemon.log.Error().Err(err).Msg("event-processor: failed to process vni event")
		}
	}()
}

func (p DefaultEventProcessor) periodicAssignmentInternal(ctx context.Context) error {
	state, err := p.daemon.db.GetFullState(ctx, p.daemon.Config, -1)
	if err != nil {
		p.daemon.log.Error().Err(err).Msg("event-processor: failed to get state on periodic assignment")
		return errors.Wrap(err, "could not get state")
	}
	nodes, err := p.daemon.db.Nodes(ctx)
	if err != nil {
		return errors.Wrap(err, "could not get nodes")
	}
	assignments := p.daemon.assignmentStrategy.Assign(nodes, state)
	for _, assignment := range assignments {
		stateType := MigrationDecided
		if assignment.Type == Failover {
			stateType = FailoverDecided
		}
		a := assignment
		go func() {
			err := p.daemon.db.pool.RunOnce(p.NewVniUpdate(a.Vni).
				LeaderState(*p.leader).
				Revision(a.State.Revision).
				Type(stateType).
				Current(a.State.Current, NodeLease).
				Next(a.Next.Name, VniLease{AttachedLeaseType, a.Next.Lease}))
			if err != nil {
				p.daemon.log.Error().Err(err).Msg("event-processor: failed to assign periodically")
			}
		}()
	}
	return nil
}

func (p DefaultEventProcessor) PeriodicAssignmentSync(ctx context.Context) {
	p.assignMutex.Lock()
	if *p.isAssigning {
		*p.willAssign = true
		p.assignMutex.Unlock()
		return
	}
	*p.isAssigning = true
	p.assignMutex.Unlock()
	err := p.periodicAssignmentInternal(ctx)
	if err != nil {
		log.Error().Err(err).Msg("event-processor: failed to run periodic assignment")
	}
	p.assignMutex.Lock()
	if *p.willAssign {
		*p.willAssign = false
		p.assignMutex.Unlock()
		err = p.periodicAssignmentInternal(ctx)
		if err != nil {
			log.Error().Err(err).Msg("event-processor: failed to run periodic assignment")
		}
		p.assignMutex.Lock()
	}
	*p.isAssigning = false
	p.assignMutex.Unlock()
}

func (p DefaultEventProcessor) PeriodicAssignmentAsync(ctx context.Context) {
	go p.PeriodicAssignmentSync(ctx)
}

func (p DefaultEventProcessor) ProcessAllVnisAsync(ctx context.Context) {
	go func() {
		var states map[uint64]*VniState
		reliably(ctx, p.daemon.log, func() error {
			var err error
			states, err = p.daemon.db.GetFullState(ctx, p.daemon.Config, -1)
			return err
		})
		select {
		case <-ctx.Done():
			return
		default:
		}
		for vni, state := range states {
			p.ProcessVniEventAsync(ctx, VniEvent{Vni: vni, State: *state})
		}
	}()
}

func (p DefaultEventProcessor) Process(ctx context.Context, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)

	*p.leader = <-leaderChan
	for {
		select {
		case <-ctx.Done():
			p.daemon.log.Debug().Msg("event-processor: context done")
			return nil
		case <-time.After(p.daemon.Config.ScanInterval):
			if p.leader.Node == p.daemon.Config.Node {
				p.PeriodicAssignmentAsync(ctx)
			}
		case newNodeEvent := <-newNodeChan:
			if p.leader.Node == p.daemon.Config.Node && newNodeEvent.Node.Name != p.daemon.Config.Node {
				p.PeriodicAssignmentAsync(ctx)
			}
		case event := <-vniChan:
			p.ProcessVniEventAsync(ctx, event)
		case newLeaderState := <-leaderChan:
			*p.leader = newLeaderState
			p.ProcessAllVnisAsync(ctx)
		}
	}
}

func (p EventProcessorWrapper) Process(ctx context.Context, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent) error {
	innerCtx, innerCancel := context.WithCancel(context.Background())
	internalVniChan := make(chan VniEvent, cap(vniChan))
	internalLeaderChan := make(chan LeaderState, 1)
	internalNewNodeChan := make(chan NewNodeEvent, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		leaderState := <-leaderChan
		internalLeaderChan <- leaderState
		for {
			select {
			case <-innerCtx.Done():
				return
			case event := <-vniChan:
				if p.beforeVniEvent(p.daemon, leaderState, event) == VerdictStop {
					p.cancel()
				}
				internalVniChan <- event
				if p.afterVniEvent(p.daemon, leaderState, event) == VerdictStop {
					p.cancel()
				}
			case leaderState = <-leaderChan:
				internalLeaderChan <- leaderState
			case newNodeEvent := <-newNodeChan:
				internalNewNodeChan <- newNodeEvent
			}
		}
	}()
	var err error
	go func() {
		err = p.eventProcessor.Process(ctx, internalVniChan, internalLeaderChan, internalNewNodeChan)
		innerCancel()
		wg.Done()
	}()
	return err
}
