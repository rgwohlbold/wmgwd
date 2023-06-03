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
	Process(ctx context.Context, daemon *Daemon, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent, timerChan <-chan TimerEvent) error
}

type DefaultEventProcessor struct{}

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

func (p DefaultEventProcessor) ProcessVniEvent(d *Daemon, leaderState LeaderState, event VniEvent, db *Database) error {
	state := event.State.Type
	current := event.State.Current
	next := event.State.Next
	isCurrent := current == d.Config.Node
	isNext := next == d.Config.Node
	isLeader := leaderState.Node == d.Config.Node

	// Failure detection and assignment
	if isLeader {
		if state == Unassigned {
			err := p.PeriodicAssignment(d, leaderState)
			if err != nil {
				log.Error().Err(err).Msg("could not assign unassigned vni")
			}
		} else {
			// All states except Unassigned and Idle need "Next"
			if next == "" && state != Idle {
				db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).RunWithRetry()
			}
			// All states except Unassigned, FailoverDecided need "Current"
			if current == "" && state != Unassigned && state != FailoverDecided {
				db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).RunWithRetry()
			}
		}
	}

	if state == MigrationDecided && isNext {
		err := d.networkStrategy.AdvertiseEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise evpn")
		}
		d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() {
			err = d.networkStrategy.AdvertiseOspf(event.Vni)
			if err != nil {
				log.Fatal().Err(err).Msg("could not advertise ospf")
			}
			d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() {
				db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfAdvertised).RunWithRetry()
			}})
		}})
	} else if state == MigrationOspfAdvertised && isCurrent {
		err := d.networkStrategy.WithdrawOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw ospf")
		}
		d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() {
			db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfWithdrawn).RunWithRetry()
		}})
	} else if state == MigrationOspfWithdrawn && isNext {
		err := d.networkStrategy.EnableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not enable arp")
		}
		db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpEnabled).RunWithRetry()
	} else if state == MigrationArpEnabled && isCurrent {
		err := d.networkStrategy.DisableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not disable arp")
		}
		db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpDisabled).RunWithRetry()
	} else if state == MigrationArpDisabled && isNext {
		err := d.networkStrategy.SendGratuitousArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not send gratuitous arp")
		}
		d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() {
			db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationGratuitousArpSent).RunWithRetry()
		}})
	} else if state == MigrationGratuitousArpSent && isCurrent {
		err := d.networkStrategy.WithdrawEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw evpn")
		}
		db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationEvpnWithdrawn).RunWithRetry()
	} else if state == MigrationEvpnWithdrawn && isNext {
		db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).RunWithRetry()
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
		d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() {
			err = d.networkStrategy.SendGratuitousArp(event.Vni)
			if err != nil {
				log.Error().Err(err).Msg("could not send gratuitous arp")
			}
			d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() {
				db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).RunWithRetry()
			}})
		}})
	}
	return nil
}

func (p DefaultEventProcessor) PeriodicAssignment(d *Daemon, leader LeaderState) error {
	state, err := d.db.GetFullState(d.Config, -1)
	if err != nil {
		log.Error().Err(err).Msg("event-processor: failed to get state on periodic assignment")
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
		err = d.db.NewVniUpdate(assignment.Vni).
			LeaderState(leader).
			Revision(assignment.State.Revision).
			Type(stateType).
			Current(assignment.State.Current, NodeLease).
			Next(assignment.Next.Name, VniLease{AttachedLeaseType, assignment.Next.Lease}).
			RunOnce()
		if err != nil {
			log.Error().Err(err).Msg("event-processor: failed to assign periodically")
		}
	}
	return errors.Wrap(err, "could not perform periodic assignment")
}

func (p DefaultEventProcessor) Process(ctx context.Context, d *Daemon, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent, timerChan <-chan TimerEvent) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)

	leader := <-leaderChan
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("event-processor: context done")
			return nil
		case <-time.After(d.Config.ScanInterval):
			if leader.Node == d.Config.Node {
				err := p.PeriodicAssignment(d, leader)
				if err != nil {
					log.Error().Err(err).Msg("event-processor: failed to perform periodic assignment")
				}
			}
		case newNodeEvent := <-newNodeChan:
			if leader.Node == d.Config.Node && newNodeEvent.Node != d.Config.Node {
				err := p.PeriodicAssignment(d, leader)
				if err != nil {
					log.Error().Err(err).Msg("event-processor: failed to perform periodic assignment")
				}
			}
		case event := <-vniChan:
			err := p.ProcessVniEvent(d, leader, event, d.db)
			if err != nil {
				log.Fatal().Err(err).Msg("event-processor: failed to process event")
			}
		case leader = <-leaderChan:
			states, err := d.db.GetFullState(d.Config, -1)
			if err != nil {
				log.Fatal().Err(err).Msg("event-processor: failed to get full state")
			}
			for vni, state := range states {
				vniChan <- VniEvent{Vni: vni, State: *state}
			}
		case timerEvent := <-timerChan:
			timerEvent.Func()
		}
	}
}

func (p EventProcessorWrapper) Process(ctx context.Context, d *Daemon, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent, timerChan <-chan TimerEvent) error {
	internalVniChan := make(chan VniEvent, cap(vniChan))
	internalLeaderChan := make(chan LeaderState)
	internalNewNodeChan := make(chan NewNodeEvent)
	internalTimerChan := make(chan TimerEvent)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		leaderState := <-leaderChan
		internalLeaderChan <- leaderState
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-vniChan:
				if p.beforeVniEvent(d, leaderState, event) == VerdictStop {
					p.cancel()
					return
				}
				internalVniChan <- event
				if p.afterVniEvent(d, leaderState, event) == VerdictStop {
					p.cancel()
					return
				}
			case leaderState = <-leaderChan:
				internalLeaderChan <- leaderState
			case newNodeEvent := <-newNodeChan:
				internalNewNodeChan <- newNodeEvent
			case timerEvent := <-timerChan:
				internalTimerChan <- timerEvent
			}
		}
	}()
	return p.eventProcessor.Process(ctx, d, internalVniChan, internalLeaderChan, internalNewNodeChan, internalTimerChan)
}
