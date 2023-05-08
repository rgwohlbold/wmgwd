package main

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"sync"
)

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
	fileInfo, err := os.Stderr.Stat()
	if err != nil {
		log.Error().Err(err).Msg("failed to stat stderr")
	} else {
		if fileInfo.Mode()&os.ModeCharDevice != 0 {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		}
	}

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
	go func() {
		signalChan := make(chan os.Signal)
		signal.Notify(signalChan, os.Interrupt)
		<-signalChan
		cancel()
	}()

	leaderChan := make(chan LeaderState)
	vniChan := make(chan VNIEvent, len(vnis))
	newNodeChan := make(chan NewNodeEvent)
	timerChan := make(chan TimerEvent)

	wg := new(sync.WaitGroup)
	RunEventIngestor[VNIEvent](ctx, node, VNIEventIngestor{}, vniChan, wg)
	RunEventIngestor[TimerEvent](ctx, node, TimerEventIngestor{}, timerChan, wg)
	RunEventIngestor[NewNodeEvent](ctx, node, NewNodeEventIngestor{}, newNodeChan, wg)
	RunEventIngestor[LeaderState](ctx, node, LeaderEventIngestor{}, leaderChan, wg)

	wg.Add(1)
	go func() {
		ProcessEvents(ctx, node, vniChan, leaderChan, newNodeChan, timerChan, vnis)
		wg.Done()
	}()

	err = RegisterNode(node)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to register node")
	}

	wg.Wait()
	log.Info().Msg("exiting, withdrawing everything")
	for _, vni := range vnis {
		err := frr.Withdraw(vni)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to withdraw")
		}
	}
}
