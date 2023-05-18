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
	if event.State.Type == Unassigned {
		if leaderState.Node == d.Config.Node {
			err := d.assignmentStrategy.Unassigned(d, event.State, leaderState, event.Vni)
			if err != nil {
				log.Error().Err(err).Msg("could not assign unassigned vni")
			}
		}
	} else if event.State.Type == MigrationDecided {
		// Acknowledge migration to use our own lease, therefore stop the timer that unassigns the vni
		if event.State.Next == d.Config.Node {
			return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(MigrationAcknowledged).Current(event.State.Current, OldLease).Next(event.State.Next, TempLease).Run()
		}
	} else if event.State.Type == MigrationAcknowledged {
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
					return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(MigrationOspfAdvertised).Current(event.State.Current, OldLease).Next(event.State.Next, OldLease).Run()
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
				return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(MigrationOspfWithdrawn).Current(event.State.Current, OldLease).Next(event.State.Next, OldLease).Run()
			}})
		}
	} else if event.State.Type == MigrationOspfWithdrawn {
		if event.State.Next == d.Config.Node {
			err := d.networkStrategy.EnableArp(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not enable arp")
			}
			return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(MigrationArpEnabled).Current(event.State.Current, OldLease).Next(event.State.Next, OldLease).Run()
		}
	} else if event.State.Type == MigrationArpEnabled {
		if event.State.Current == d.Config.Node {
			err := d.networkStrategy.DisableArp(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not disable arp")
			}
			return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(MigrationArpDisabled).Current(event.State.Current, OldLease).Next(event.State.Next, OldLease).Run()
		}
	} else if event.State.Type == MigrationArpDisabled {
		if event.State.Next == d.Config.Node {
			err := d.networkStrategy.SendGratuitousArp(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not send gratuitous arp")
			}
			d.timerEventIngestor.Enqueue(d.Config.MigrationTimeout, TimerEvent{Func: func() error {
				return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(MigrationGratuitousArpSent).Current(event.State.Current, OldLease).Next(event.State.Next, OldLease).Run()
			}})
		}
	} else if event.State.Type == MigrationGratuitousArpSent {
		if event.State.Current == d.Config.Node {
			err := d.networkStrategy.WithdrawEvpn(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not withdraw evpn")
			}
			return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(MigrationEvpnWithdrawn).Current(event.State.Current, OldLease).Next(event.State.Next, OldLease).Run()
		}
	} else if event.State.Type == MigrationEvpnWithdrawn {
		if event.State.Next == d.Config.Node {
			return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(Idle).Current(event.State.Next, NodeLease).Next("", NodeLease).Run()
		}
	} else if event.State.Type == FailoverDecided {
		if event.State.Next == d.Config.Node {
			return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(FailoverAcknowledged).Current("", NoLease).Next(event.State.Next, NodeLease).Run()
		}
	} else if event.State.Type == FailoverAcknowledged {
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
					return db.NewVniUpdate(event.Vni).OldCounter(event.State.Counter).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).Run()
				}})
				return nil
			}})
		}
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
			for _, vni := range d.Config.Vnis {
				state, err := d.db.GetState(vni, -1)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to get state")
				}
				vniChan <- VniEvent{Vni: vni, State: state}
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
