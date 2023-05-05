package main

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"time"
)

type VNIEvent struct {
	State VNIState
	VNI   int
}

func ProcessEvents(ctx context.Context, node string, vniChan chan VNIEvent, leaderChan <-chan LeaderEvent, newNodeChan <-chan NewNodeEvent, timerChan <-chan TimerEvent, vnis []int) {
	db, err := NewDatabase(node)
	if err != nil {
		log.Fatal().Err(err).Msg("event-processor: failed to connect to database")
	}
	frr := NewFRRClient()

	leader := <-leaderChan
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("event-processor: context done")
			return
		case event := <-vniChan:
			err := ProcessVNIEvent(node, leader, event, frr, db)
			if err != nil {
				log.Fatal().Err(err).Msg("event-processor: failed to process event")
			}
		case leader = <-leaderChan:
			for _, vni := range vnis {
				state, err := db.GetState(vni)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to get state")
				}
				vniChan <- VNIEvent{VNI: vni, State: state}
			}
		case newNodeEvent := <-newNodeChan:
			if leader.IsLeader && node != newNodeEvent.Node {
				state, err := db.GetState(vnis[0])
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to get state")
				}
				err = db.SetMigrationDecided(vnis[0], state.Current, newNodeEvent.Node)
				if err != nil {
					log.Fatal().Err(err).Msg("event-processor: failed to set migration decided")
				}
			}
		case timerEvent := <-timerChan:
			err := db.setVNIState(timerEvent.VNI, VNIState{
				Type:    Idle,
				Current: node,
				Next:    "",
			})
			if err != nil {
				log.Fatal().Err(err).Msg("event-processor: failed to set vni state")
			}
		}
	}
}

func RegisterNode(node string) error {
	db, err := NewDatabase(node)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Register(node)
	if err != nil {
		return err
	}
	log.Info().Msg("registered node")
	return nil
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

	ctx, cancel := context.WithCancel(context.Background())

	log.Info().Msg("starting leader election loop")
	leaderChan := make(chan LeaderEvent)
	eventChan := make(chan VNIEvent, len(vnis))
	newNodeChan := make(chan NewNodeEvent)
	timerChan := make(chan TimerEvent)

	go GenerateWatchEvents(ctx, node, eventChan)
	go GenerateTimerEvents(ctx, timerChan)
	go GenerateNewNodeEvents(ctx, newNodeChan)
	go GenerateLeaderEvents(ctx, node, leaderChan)
	go ProcessEvents(ctx, node, eventChan, leaderChan, newNodeChan, timerChan, vnis)

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, os.Interrupt)

	<-time.After(1 * time.Second)
	err := RegisterNode(node)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to register node")
	}

	// Assure that all listeners have been set up when we register
	<-time.After(1 * time.Second)

	<-signalChan
	cancel()

	time.Sleep(10 * time.Second)
}
