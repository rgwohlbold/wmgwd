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
		ScanInterval:     1 * time.Second,
		Node:             os.Args[1],
		Vnis:             []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
		MigrationTimeout: 5 * time.Second,
		DrainOnShutdown:  true,
	}, NewSystemNetworkStrategy(), AssignConsistentHashing{}).Run(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to run daemon")
	}
}
