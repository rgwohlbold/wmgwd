package main

import (
	"context"
	"github.com/rs/zerolog/log"
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
			err := frr.Advertise(event.Vni)
			if err != nil {
				return err
			}
			return db.setVniState(event.Vni, VniState{
				Type:    MigrationInterfacesCreated,
				Current: event.State.Current,
				Next:    node,
			}, event.State, leaderState)
		}
	} else if event.State.Type == MigrationInterfacesCreated {
		if node == event.State.Current {
			err := frr.Withdraw(event.Vni)
			if err != nil {
				return err
			}
			return db.setVniState(event.Vni, VniState{
				Type:    MigrationCostReduced,
				Current: node,
				Next:    event.State.Next,
			}, event.State, leaderState)
		}
	} else if event.State.Type == MigrationCostReduced {
		if node == event.State.Next {
			AddMigrationTimer(event.Vni)
		}
	} else if event.State.Type == FailoverDecided {
		if node == event.State.Next {
			err := frr.Advertise(event.Vni)
			if err != nil {
				return err
			}
			return db.setVniState(event.Vni, VniState{
				Type:    Idle,
				Current: node,
			}, event.State, leaderState)
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
		case newNodeEvent := <-newNodeChan:
			if leader.Node == node && node != newNodeEvent.Node {
				state, err := db.GetState(vnis[0])
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to get state")
				}
				err = db.setVniState(vnis[0], VniState{
					Type:    MigrationDecided,
					Current: node,
					Next:    newNodeEvent.Node,
				}, state, leader)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to set migration decided")
				}
			}
		case timerEvent := <-timerChan:
			err = db.setVniState(timerEvent.Vni, VniState{
				Type:    Idle,
				Current: node,
				Next:    "",
			}, timerEvent.State, leader)
			if err != nil {
				log.Fatal().Err(err).Msg("event-processor: failed to set vni state")
			}
		}
	}
}
