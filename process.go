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

func (_ DefaultEventProcessor) ProcessVniEvent(d *Daemon, leaderState LeaderState, event VniEvent, db *Database) error {
	state := event.State.Type
	current := event.State.Current
	next := event.State.Next
	isCurrent := current == d.Config.Node
	isNext := next == d.Config.Node
	isLeader := leaderState.Node == d.Config.Node

	// Failure detection and assignment
	if isLeader {
		if state == Unassigned {
			err := d.assignmentStrategy.Unassigned(d, event.State, leaderState, event.Vni)
			if err != nil {
				log.Error().Err(err).Msg("could not assign unassigned vni")
			}
		} else {
			// All states except Unassigned and Idle need "Next"
			if next == "" && state != Idle {
				return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).Run()
			}
			// All states except Unassigned, FailoverDecided need "Current"
			if current == "" && state != Unassigned && state != FailoverDecided {
				return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).Run()
			}
		}
	}

	if state == MigrationDecided && isNext {
		err := d.networkStrategy.AdvertiseEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise evpn")
		}
		d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() error {
			err = d.networkStrategy.AdvertiseOspf(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not advertise ospf")
			}
			d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() error {
				return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfAdvertised).Run()
			}})
			return nil
		}})
	} else if state == MigrationOspfAdvertised && isCurrent {
		err := d.networkStrategy.WithdrawOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw ospf")
		}
		d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() error {
			return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfWithdrawn).Run()
		}})
	} else if state == MigrationOspfWithdrawn && isNext {
		err := d.networkStrategy.EnableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not enable arp")
		}
		return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpEnabled).Run()
	} else if state == MigrationArpEnabled && isCurrent {
		err := d.networkStrategy.DisableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not disable arp")
		}
		return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpDisabled).Run()
	} else if state == MigrationArpDisabled && isNext {
		err := d.networkStrategy.SendGratuitousArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not send gratuitous arp")
		}
		d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() error {
			return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationGratuitousArpSent).Run()
		}})
	} else if state == MigrationGratuitousArpSent && isCurrent {
		err := d.networkStrategy.WithdrawEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw evpn")
		}
		return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationEvpnWithdrawn).Run()
	} else if state == MigrationEvpnWithdrawn && isNext {
		return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).Run()
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
		d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() error {
			err = d.networkStrategy.SendGratuitousArp(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not send gratuitous arp")
			}
			d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() error {
				return db.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).Run()
			}})
			return nil
		}})
	}
	return nil
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
				for _, vni := range d.Config.Vnis {
					state, err := d.db.GetState(vni, -1)
					if err != nil {
						log.Fatal().Err(err).Msg("event-processor: failed to get state")
					}
					err = d.assignmentStrategy.Periodical(d, state, leader, vni)
					if err != nil {
						log.Error().Err(err).Msg("event-processor: failed to process periodical")
					}
				}
			}
		case newNodeEvent := <-newNodeChan:
			if leader.Node == d.Config.Node && newNodeEvent.Node != d.Config.Node {
				log.Info().Str("node", newNodeEvent.Node).Msg("event-processor: new node")
				for _, vni := range d.Config.Vnis {
					state, err := d.db.GetState(vni, -1)
					if err != nil {
						log.Fatal().Err(err).Msg("event-processor: failed to get state")
					}
					err = d.assignmentStrategy.Periodical(d, state, leader, vni)
					if err != nil {
						log.Error().Err(err).Msg("event-processor: failed to process new node")
					}
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
			err := timerEvent.Func()
			if err != nil {
				log.Fatal().Err(err).Msg("event-processor: failed to execute timer event")
			}
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
