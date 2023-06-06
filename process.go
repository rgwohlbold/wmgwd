package main

import (
	"context"
	"github.com/pkg/errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type EventProcessor interface {
	Process(ctx context.Context, daemon *Daemon, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent) error
}

type DefaultEventProcessor struct {
	leader *LeaderState
}

type Verdict int

const (
	VerdictContinue Verdict = iota
	VerdictStop
)

type EventProcessorWrapper struct {
	cancel         context.CancelFunc
	eventProcessor EventProcessor
	afterVniEvent  func(*Daemon, LeaderState, VniEvent) Verdict
	beforeVniEvent func(*Daemon, LeaderState, VniEvent) Verdict
}

func NoopVniEvent(_ *Daemon, _ LeaderState, _ VniEvent) Verdict {
	return VerdictContinue
}

func NewDefaultEventProcessor() *DefaultEventProcessor {
	return &DefaultEventProcessor{
		leader: &LeaderState{},
	}
}

func (p DefaultEventProcessor) ProcessVniEventSync(d *Daemon, event VniEvent) error {
	state := event.State.Type
	current := event.State.Current
	next := event.State.Next
	isCurrent := current == d.Config.Node
	isNext := next == d.Config.Node
	isLeader := p.leader.Node == d.Config.Node

	// Failure detection and assignment
	if isLeader {
		if state == Unassigned {
			err := p.PeriodicAssignment(d)
			if err != nil {
				return errors.Wrap(err, "could not assign unassigned vni")
			}
		} else {
			// All states except Unassigned and Idle need "Next"
			if next == "" && state != Idle {
				d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).RunWithRetry()
			}
			// All states except Unassigned, FailoverDecided need "Current"
			if current == "" && state != Unassigned && state != FailoverDecided {
				d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).RunWithRetry()
			}
		}
	}

	if state == MigrationDecided && isNext {
		err := d.networkStrategy.AdvertiseEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise evpn")
		}
		time.Sleep(d.Config.MigrationTimeout)
		err = d.networkStrategy.AdvertiseOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise ospf")
		}
		time.Sleep(d.Config.MigrationTimeout)
		d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfAdvertised).RunWithRetry()
	} else if state == MigrationOspfAdvertised && isCurrent {
		err := d.networkStrategy.WithdrawOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw ospf")
		}
		time.Sleep(d.Config.MigrationTimeout)
		d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfWithdrawn).RunWithRetry()
	} else if state == MigrationOspfWithdrawn && isNext {
		err := d.networkStrategy.EnableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not enable arp")
		}
		d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpEnabled).RunWithRetry()
	} else if state == MigrationArpEnabled && isCurrent {
		err := d.networkStrategy.DisableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not disable arp")
		}
		d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpDisabled).RunWithRetry()
	} else if state == MigrationArpDisabled && isNext {
		err := d.networkStrategy.SendGratuitousArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not send gratuitous arp")
		}
		time.Sleep(d.Config.MigrationTimeout)
		d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationGratuitousArpSent).RunWithRetry()
	} else if state == MigrationGratuitousArpSent && isCurrent {
		err := d.networkStrategy.WithdrawEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw evpn")
		}
		d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationEvpnWithdrawn).RunWithRetry()
	} else if state == MigrationEvpnWithdrawn && isNext {
		d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).RunWithRetry()
	} else if state == FailoverDecided && isNext {
		err := d.networkStrategy.AdvertiseEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise evpn")
		}
		err = d.networkStrategy.AdvertiseOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise ospf")
		}
		err = d.networkStrategy.EnableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not enable arp")
		}
		time.Sleep(d.Config.MigrationTimeout)
		err = d.networkStrategy.SendGratuitousArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not send gratuitous arp")
		}
		time.Sleep(d.Config.MigrationTimeout)
		d.db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).RunWithRetry()
	}
	return nil
}

func (p DefaultEventProcessor) ProcessVniEventAsync(d *Daemon, event VniEvent) {
	go func() {
		err := p.ProcessVniEventSync(d, event)
		if err != nil {
			d.log.Error().Err(err).Msg("event-processor: failed to process vni event")
		}
	}()
}

func (p DefaultEventProcessor) PeriodicAssignment(d *Daemon) error {
	state, err := d.db.GetFullState(d.Config, -1)
	if err != nil {
		d.log.Error().Err(err).Msg("event-processor: failed to get state on periodic assignment")
		return errors.Wrap(err, "could not get state")
	}
	nodes, err := d.db.Nodes()
	if err != nil {
		return errors.Wrap(err, "could not get nodes")
	}
	assignments := d.assignmentStrategy.Assign(d, nodes, state)
	for _, assignment := range assignments {
		stateType := MigrationDecided
		if assignment.Type == Failover {
			stateType = FailoverDecided
		}
		a := assignment
		go func() {
			err := d.db.NewVniUpdate(a.Vni).
				LeaderState(*p.leader).
				Revision(a.State.Revision).
				Type(stateType).
				Current(a.State.Current, NodeLease).
				Next(a.Next.Name, VniLease{AttachedLeaseType, a.Next.Lease}).
				RunOnce()
			if err != nil {
				d.log.Error().Err(err).Msg("event-processor: failed to assign periodically")
			}
		}()
	}
	return nil
}

func (p DefaultEventProcessor) Process(ctx context.Context, d *Daemon, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)

	*p.leader = <-leaderChan
	for {
		select {
		case <-ctx.Done():
			d.log.Debug().Msg("event-processor: context done")
			return nil
		case <-time.After(d.Config.ScanInterval):
			if p.leader.Node == d.Config.Node {
				err := p.PeriodicAssignment(d)
				if err != nil {
					d.log.Error().Err(err).Msg("event-processor: failed to perform periodic assignment")
				}
			}
		case newNodeEvent := <-newNodeChan:
			if p.leader.Node == d.Config.Node && newNodeEvent.Node.Name != d.Config.Node {
				err := p.PeriodicAssignment(d)
				if err != nil {
					d.log.Error().Err(err).Msg("event-processor: failed to perform periodic assignment")
				}
			}
		case event := <-vniChan:
			p.ProcessVniEventAsync(d, event)
		case newLeaderState := <-leaderChan:
			*p.leader = newLeaderState
			states, err := d.db.GetFullState(d.Config, -1)
			if err != nil {
				d.log.Fatal().Err(err).Msg("event-processor: failed to get full state")
			}
			for vni, state := range states {
				p.ProcessVniEventAsync(d, VniEvent{Vni: vni, State: *state})
			}
		}
	}
}

func (p EventProcessorWrapper) Process(ctx context.Context, d *Daemon, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent) error {
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
				if p.beforeVniEvent(d, leaderState, event) == VerdictStop {
					p.cancel()
				}
				internalVniChan <- event
				if p.afterVniEvent(d, leaderState, event) == VerdictStop {
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
		err = p.eventProcessor.Process(ctx, d, internalVniChan, internalLeaderChan, internalNewNodeChan)
		innerCancel()
		wg.Done()
	}()
	return err
}
