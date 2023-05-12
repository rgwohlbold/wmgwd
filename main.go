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
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		signalChan := make(chan os.Signal)
		signal.Notify(signalChan, os.Interrupt)
		<-signalChan
		cancel()
	}()
	err = NewDaemon(Configuration{
		Node:             os.Args[1],
		Vnis:             []uint64{100},
		MigrationTimeout: 5 * time.Second,
	}, NewSystemNetworkStrategy()).Run(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to run daemon")
	}
}
