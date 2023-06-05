package main

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(2) // vtysh and go stuff
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zerolog.TimeFieldFormat = time.RFC3339Nano
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
	ctx, cancel := context.WithCancel(context.Background())

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		cancel()
	}()

	vnis := make([]uint64, 0, 1000)
	for i := uint64(1); i < 1000; i++ {
		vnis = append(vnis, i)
	}
	err = NewDaemon(Configuration{
		ScanInterval:     1 * time.Second,
		Node:             os.Args[1],
		Vnis:             vnis,
		MigrationTimeout: 1 * time.Second,
		DrainOnShutdown:  true,
		Report:           false,
	}, NewMockNetworkStrategy(), AssignConsistentHashing{}).Run(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to run daemon")
	}
}
