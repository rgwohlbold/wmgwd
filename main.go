package main

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

type VNIEvent struct {
	State VNIState
	VNI   int
}

func ProcessEvents(node string, vniChan chan VNIEvent, leaderChan <-chan LeaderEvent, newNodeChan <-chan NewNodeEvent, timerChan <-chan TimerEvent, frr *FRRClient, db *Database, vnis []int) {
	leader := <-leaderChan
	for {
		select {
		case event := <-vniChan:
			err := ProcessVNIEvent(node, leader, event, frr, db)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to process event")
			}
		case leader = <-leaderChan:
			GenerateLeaderChangeEvents(vniChan, db, vnis)
		case newNodeEvent := <-newNodeChan:
			if leader.IsLeader && node != newNodeEvent.Node {
				state, err := db.GetState(vnis[0])
				if err != nil {
					log.Fatal().Err(err).Msg("failed to get state")
				}
				err = db.SetMigrationDecided(vnis[0], state.Current, newNodeEvent.Node)
				if err != nil {
					log.Fatal().Err(err).Msg("failed to set migration decided")
				}
			}
		case timerEvent := <-timerChan:
			state, err := db.GetState(timerEvent.VNI)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to get state")
			}
			err = db.setVNIState(timerEvent.VNI, VNIState{
				Type:    Idle,
				Current: state.Next,
				Next:    "",
			})
			if err != nil {
				log.Fatal().Err(err).Msg("failed to set vni state")
			}
		}
	}
}

func main() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	if len(os.Args) != 2 {
		log.Fatal().Msg("usage: wmgwd <node>")
	}
	node := os.Args[1]

	frr := NewFRRClient()

	vnis := []int{100, 200, 300}
	for _, vni := range vnis {
		err := frr.Withdraw(vni)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to withdraw")
		}
	}

	ctx := context.Background()

	db, err := NewDatabase(node)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer db.Close()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to register node")
	}

	log.Info().Msg("starting leader election loop")
	leaderChan := make(chan LeaderEvent)
	eventChan := make(chan VNIEvent, len(vnis))
	newNodeChan := make(chan NewNodeEvent)
	timerChan := make(chan TimerEvent)

	go GenerateWatchEvents(node, eventChan)
	go GenerateTimerEvents(ctx, timerChan)
	go GenerateNewNodeEvents(ctx, newNodeChan)
	go GenerateLeaderEvents(ctx, db, node, leaderChan)
	go ProcessEvents(node, eventChan, leaderChan, newNodeChan, timerChan, frr, db, vnis)

	// Assure that all listeners have been set up when we register
	<-time.After(1 * time.Second)
	err = db.Register(node)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to register node")
	}
	log.Info().Msg("registered node")

	<-ctx.Done()
}
