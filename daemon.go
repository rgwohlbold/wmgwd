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

type Configuration struct {
	Node string
	Vnis []uint64
}

type Daemon struct {
	Config               Configuration
	networkStrategy      NetworkStrategy
	vniEventIngestor     VniEventIngestor
	timerEventIngestor   TimerEventIngestor
	newNodeEventIngestor NewNodeEventIngestor
	leaderEventIngestor  LeaderEventIngestor
}

type EventIngestor[E any] interface {
	Ingest(ctx context.Context, daemon *Daemon, eventChan chan<- E, setupChan chan<- struct{})
}

func runEventIngestor[E any](ctx context.Context, daemon *Daemon, ingestor EventIngestor[E], eventChan chan<- E, wg *sync.WaitGroup) {
	wg.Add(1)
	setupChan := make(chan struct{})
	go func() {
		ingestor.Ingest(ctx, daemon, eventChan, setupChan)
		wg.Done()
	}()
	<-setupChan
}

func NewDaemon(config Configuration, networkStrategy NetworkStrategy) *Daemon {
	return &Daemon{
		Config:               config,
		networkStrategy:      networkStrategy,
		vniEventIngestor:     VniEventIngestor{},
		timerEventIngestor:   NewTimerEventIngestor(),
		newNodeEventIngestor: NewNodeEventIngestor{},
		leaderEventIngestor:  LeaderEventIngestor{},
	}
}

func (d *Daemon) Register() error {
	db, err := NewDatabase(d.Config)
	if err != nil {
		return errors.Wrap(err, "could not open database")
	}
	defer db.Close()

	err = db.Register(d.Config.Node)
	if err != nil {
		return errors.Wrap(err, "could not register node")
	}
	log.Info().Msg("registered node")
	return nil
}

func (d *Daemon) Run(ctx context.Context) error {
	for _, vni := range d.Config.Vnis {
		err := d.networkStrategy.WithdrawEvpn(vni)
		if err != nil {
			return errors.Wrap(err, "failed to withdraw evpn")
		}
		err = d.networkStrategy.WithdrawOspf(vni)
		if err != nil {
			return errors.Wrap(err, "failed to withdraw ospf")
		}
		err = d.networkStrategy.DisableArp(vni)
		if err != nil {
			return errors.Wrap(err, "failed to disable arp")
		}
	}

	//ctx, cancel := context.WithCancel(context.Background())
	//go func() {
	//	signalChan := make(chan os.Signal)
	//	signal.Notify(signalChan, os.Interrupt)
	//	<-signalChan
	//	cancel()
	//}()

	leaderChan := make(chan LeaderState)
	vniChan := make(chan VniEvent, len(d.Config.Vnis))
	newNodeChan := make(chan NewNodeEvent)
	timerChan := make(chan TimerEvent)

	wg := new(sync.WaitGroup)
	//timerEventIngestor := NewTimerEventIngestor()
	runEventIngestor[VniEvent](ctx, d, d.vniEventIngestor, vniChan, wg)
	runEventIngestor[TimerEvent](ctx, d, d.timerEventIngestor, timerChan, wg)
	runEventIngestor[NewNodeEvent](ctx, d, d.newNodeEventIngestor, newNodeChan, wg)
	runEventIngestor[LeaderState](ctx, d, d.leaderEventIngestor, leaderChan, wg)

	wg.Add(1)
	go func() {
		err := d.ProcessEvents(ctx, vniChan, leaderChan, newNodeChan, timerChan)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to process events")
		}
		wg.Done()
	}()

	err := d.Register()
	if err != nil {
		return errors.Wrap(err, "could not register")
	}

	wg.Wait()
	log.Info().Msg("exiting, withdrawing everything")
	for _, vni := range d.Config.Vnis {
		err = d.networkStrategy.WithdrawEvpn(vni)
		if err != nil {
			return errors.Wrap(err, "failed to withdraw evpn")
		}
		err = d.networkStrategy.WithdrawOspf(vni)
		if err != nil {
			return errors.Wrap(err, "failed to withdraw ospf")
		}
	}
	return nil
}

func (d *Daemon) ProcessVniEvent(leaderState LeaderState, event VniEvent, db *Database) error {
	if event.State.Type == Unassigned {
		if leaderState.Node == d.Config.Node {
			return db.setVniState(event.Vni, VniState{
				Type:    FailoverDecided,
				Current: "",
				Next:    d.Config.Node,
			}, event.State, leaderState)
		}
	} else if event.State.Type == MigrationDecided {
		if event.State.Next == d.Config.Node {
			err := d.networkStrategy.AdvertiseEvpn(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not advertise evpn")
			}
			d.timerEventIngestor.Enqueue(TimerEvent{Func: func() error {
				err = d.networkStrategy.AdvertiseOspf(event.Vni)
				if err != nil {
					return errors.Wrap(err, "could not advertise ospf")
				}
				d.timerEventIngestor.Enqueue(TimerEvent{Func: func() error {
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
			go func() {
				time.Sleep(MigrationTimeout)
				err = db.setVniState(event.Vni, VniState{
					Type:    MigrationOspfWithdrawn,
					Current: event.State.Current,
					Next:    event.State.Next,
				}, event.State, leaderState)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to set vni state")
				}
			}()

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
			d.timerEventIngestor.Enqueue(TimerEvent{Func: func() error {
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
			d.timerEventIngestor.Enqueue(TimerEvent{Func: func() error {
				err = d.networkStrategy.SendGratuitousArp(event.Vni)
				if err != nil {
					return errors.Wrap(err, "could not send gratuitous arp")
				}
				d.timerEventIngestor.Enqueue(TimerEvent{Func: func() error {
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
func (d *Daemon) ProcessEvents(ctx context.Context, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent, timerChan <-chan TimerEvent) error {
	db, err := NewDatabase(d.Config)
	if err != nil {
		log.Fatal().Err(err).Msg("event-processor: failed to connect to database")
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)

	leader := <-leaderChan
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("event-processor: context done")
			return nil
		case event := <-vniChan:
			err = d.ProcessVniEvent(leader, event, db)
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
		case newNodeEvent := <-newNodeChan:
			if leader.Node == d.Config.Node && newNodeEvent.Node != d.Config.Node {
				log.Info().Str("node", newNodeEvent.Node).Msg("event-processor: new node")
			}
		case timerEvent := <-timerChan:
			err = timerEvent.Func()
			if err != nil {
				log.Fatal().Err(err).Msg("event-processor: failed to execute timer event")
			}
		}
	}
}
