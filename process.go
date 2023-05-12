package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func ProcessVniEvent(node string, leaderState LeaderState, event VniEvent, frr *FRRClient, db *Database) error {
	if event.State.Type == Unassigned {
		if leaderState.Node == node {
			return db.setVniState(event.Vni, VniState{
				Type:    FailoverDecided,
				Current: "",
				Next:    node,
			}, event.State, leaderState)
		}
	} else if event.State.Type == MigrationDecided {
		if node == event.State.Next {
			err := frr.AdvertiseEvpn(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not advertise evpn")
			}
			QueueTimerEvent(TimerEvent{Func: func() error {
				err = frr.AdvertiseOspf(event.Vni)
				if err != nil {
					return errors.Wrap(err, "could not advertise ospf")
				}
				QueueTimerEvent(TimerEvent{Func: func() error {
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
		if node == event.State.Current {
			err := frr.WithdrawOspf(event.Vni)
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
		if node == event.State.Next {
			err := frr.EnableArp(event.Vni)
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
		if node == event.State.Current {
			err := frr.DisableArp(event.Vni)
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
		if node == event.State.Next {
			err := frr.SendGratuitousArp(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not send gratuitous arp")
			}
			QueueTimerEvent(TimerEvent{Func: func() error {
				return db.setVniState(event.Vni, VniState{
					Type:    MigrationGratuitousArpSent,
					Current: event.State.Current,
					Next:    event.State.Next,
				}, event.State, leaderState)
			}})
		}
	} else if event.State.Type == MigrationGratuitousArpSent {
		if node == event.State.Current {
			err := frr.WithdrawEvpn(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not withdraw evpn")
			}
			return db.setVniState(event.Vni, VniState{
				Type:    Idle,
				Current: event.State.Next,
			}, event.State, leaderState)
		}
	} else if event.State.Type == FailoverDecided {
		if node == event.State.Next {
			err := frr.AdvertiseEvpn(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not advertise evpn")
			}
			err = frr.AdvertiseOspf(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not advertise ospf")
			}
			err = frr.EnableArp(event.Vni)
			if err != nil {
				return errors.Wrap(err, "could not enable arp")
			}
			QueueTimerEvent(TimerEvent{Func: func() error {
				err = frr.SendGratuitousArp(event.Vni)
				if err != nil {
					return errors.Wrap(err, "could not send gratuitous arp")
				}
				QueueTimerEvent(TimerEvent{Func: func() error {
					return db.setVniState(event.Vni, VniState{
						Type:    Idle,
						Current: node,
					}, event.State, leaderState)
				}})
				return nil
			}})
		}
	}
	return nil
}
func ProcessEvents(ctx context.Context, node string, vniChan chan VniEvent, leaderChan <-chan LeaderState, newNodeChan <-chan NewNodeEvent, timerChan <-chan TimerEvent, vnis []uint64) {
	db, err := NewDatabase(node)
	if err != nil {
		log.Fatal().Err(err).Msg("event-processor: failed to connect to database")
	}
	frr := NewFRRClient()
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)

	leader := <-leaderChan
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("event-processor: context done")
			return
		case event := <-vniChan:
			err := ProcessVniEvent(node, leader, event, frr, db)
			if err != nil {
				log.Fatal().Err(err).Msg("event-processor: failed to process event")
			}
		case leader = <-leaderChan:
			for _, vni := range vnis {
				state, err := db.GetState(vni)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to get state")
				}
				vniChan <- VniEvent{Vni: vni, State: state}
			}
		case sig := <-c:
			if sig == syscall.SIGUSR1 {
				state, err := db.GetState(vnis[0])
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
				err = db.setVniState(vnis[0], VniState{
					Type:    MigrationDecided,
					Current: state.Current,
					Next:    newNode,
				}, state, leader)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to set migration decided")
				}
			}
		case newNodeEvent := <-newNodeChan:
			if leader.Node == node && node != newNodeEvent.Node {
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
