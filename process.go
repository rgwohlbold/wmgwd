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
}

func (_ DefaultEventProcessor) ProcessVniEvent(d *Daemon, leaderState LeaderState, event VniEvent, db *Database) error {
	if event.State.Type == Unassigned {
		if leaderState.Node == d.Config.Node {
			return d.assignmentStrategy.Unassigned(d, db, leaderState, event.Vni)
		}
	} else if event.State.Type == MigrationDecided {
		if event.State.Next == d.Config.Node {
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
					return db.setVniState(event.Vni, VniState{
						Type:    MigrationOspfAdvertised,
						Current: event.State.Current,
						Next:    event.State.Next,
					}, event.State, leaderState)
				}})
				return nil
			}})
		}
	} else if event.State.Type == MigrationOspfAdvertised {
		if event.State.Current == d.Config.Node {
			err := d.networkStrategy.WithdrawOspf(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not withdraw ospf")
			}
			d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() error {
				return db.setVniState(event.Vni, VniState{
					Type:    MigrationOspfWithdrawn,
					Current: event.State.Current,
					Next:    event.State.Next,
				}, event.State, leaderState)
			}})
		}
	} else if event.State.Type == MigrationOspfWithdrawn {
		if event.State.Next == d.Config.Node {
			err := d.networkStrategy.EnableArp(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not enable arp")
			}
			return db.setVniState(event.Vni, VniState{
				Type:    MigrationArpEnabled,
				Current: event.State.Current,
				Next:    event.State.Next,
			}, event.State, leaderState)
		}
	} else if event.State.Type == MigrationArpEnabled {
		if event.State.Current == d.Config.Node {
			err := d.networkStrategy.DisableArp(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not disable arp")
			}
			return db.setVniState(event.Vni, VniState{
				Type:    MigrationArpDisabled,
				Current: event.State.Current,
				Next:    event.State.Next,
			}, event.State, leaderState)
		}
	} else if event.State.Type == MigrationArpDisabled {
		if event.State.Next == d.Config.Node {
			err := d.networkStrategy.SendGratuitousArp(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not send gratuitous arp")
			}
			d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() error {
				return db.setVniState(event.Vni, VniState{
					Type:    MigrationGratuitousArpSent,
					Current: event.State.Current,
					Next:    event.State.Next,
				}, event.State, leaderState)
			}})
		}
	} else if event.State.Type == MigrationGratuitousArpSent {
		if event.State.Current == d.Config.Node {
			err := d.networkStrategy.WithdrawEvpn(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not withdraw evpn")
			}
			return db.setVniState(event.Vni, VniState{
				Type:    Idle,
				Current: event.State.Next,
			}, event.State, leaderState)
		}
	} else if event.State.Type == FailoverDecided {
		if event.State.Next == d.Config.Node {
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
					return db.setVniState(event.Vni, VniState{
						Type:    Idle,
						Current: d.Config.Node,
					}, event.State, leaderState)
				}})
				return nil
			}})
		}
	}
	return nil
}

func (p DefaultEventProcessor) Process(ctx context.Context, d *Daemon, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent, timerChan <-chan TimerEvent) error {
	db, err := NewDatabase(ctx, d.Config)
	if err != nil {
		log.Fatal().Err(err).Msg("event-processor: failed to connect to database")
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)

	leader := <-leaderChan
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("event-processor: context done")
			return nil
		case <-time.After(d.Config.ScanInterval):
			for _, vni := range d.Config.Vnis {
				err = d.assignmentStrategy.Periodical(d, db, leader, vni)
				if err != nil {
					log.Error().Err(err).Msg("event-processor: failed to process periodical")
				}
			}
		case newNodeEvent := <-newNodeChan:
			if leader.Node == d.Config.Node && newNodeEvent.Node != d.Config.Node {
				log.Info().Str("node", newNodeEvent.Node).Msg("event-processor: new node")
				for _, vni := range d.Config.Vnis {
					err = d.assignmentStrategy.Periodical(d, db, leader, vni)
					if err != nil {
						log.Error().Err(err).Msg("event-processor: failed to process new node")
					}
				}
			}
		case event := <-vniChan:
			err = p.ProcessVniEvent(d, leader, event, db)
			if err != nil {
				log.Fatal().Err(err).Msg("event-processor: failed to process event")
			}
		case leader = <-leaderChan:
			for _, vni := range d.Config.Vnis {
				state, err := db.GetState(vni)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to get state")
				}
				vniChan <- VniEvent{Vni: vni, State: state}
			}
		case sig := <-c:
			if sig == syscall.SIGUSR1 {
				vni := d.Config.Vnis[0]
				var state VniState
				state, err = db.GetState(vni)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to get state")
				}
				if state.Type != Idle {
					continue
				}
				newNode := "h1"
				if state.Current == "h1" {
					newNode = "h2"
				}
				err = db.setVniState(vni, VniState{
					Type:    MigrationDecided,
					Current: state.Current,
					Next:    newNode,
				}, state, leader)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to set migration decided")
				}
			}
		case timerEvent := <-timerChan:
			err = timerEvent.Func()
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
		for {
			leaderState := <-leaderChan
			internalLeaderChan <- leaderState
			select {
			case <-ctx.Done():
				return
			case event := <-vniChan:
				internalVniChan <- event
				if p.afterVniEvent(d, leaderState, event) == VerdictStop {
					p.cancel()
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
