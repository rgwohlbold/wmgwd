package main

import (
	"context"
	"flag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func ParseConfiguration() (Configuration, NetworkStrategy) {
	name := flag.String("name", "", "node name")
	minVni := flag.Uint64("minvni", 1, "minimum vni")
	maxVni := flag.Uint64("maxvni", 100, "maximum vni")
	mockNetwork := flag.Bool("mock", false, "use mock network strategy")
	uidString := flag.String("uids", "", "comma-separated list of uids")
	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()
	if *name == "" {
		log.Fatal().Msg("name is required")
	}
	var uids []uint64
	if *uidString == "" {
		uids = make([]uint64, NumConsistentHashingUids)
		for i := range uids {
			uids[i] = rand.Uint64()
		}
	} else {
		for _, part := range strings.Split(*uidString, ",") {
			uid, err := strconv.ParseUint(part, 10, 64)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to parse uid")
			}
			uids = append(uids, uid)
		}
	}
	var ns NetworkStrategy
	ns = NewMockNetworkStrategy()
	if *mockNetwork {
		ns = NewMockNetworkStrategy()
	} else {
		ns = NewSystemNetworkStrategy()
	}
	var vnis []uint64
	for i := *minVni; i <= *maxVni; i++ {
		vnis = append(vnis, i)
	}
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	return Configuration{
		Name:             *name,
		Uids:             uids,
		Vnis:             vnis,
		MigrationTimeout: 10 * time.Second,
		FailoverTimeout:  30 * time.Second,
		ScanInterval:     30 * time.Second,
		DrainOnShutdown:  true,
	}, ns
}

func main() {
	runtime.GOMAXPROCS(2) // vtysh and go stuff
	zerolog.TimeFieldFormat = time.RFC3339Nano
	fileInfo, err := os.Stderr.Stat()
	if err != nil {
		log.Error().Err(err).Msg("failed to stat stderr")
	} else {
		if fileInfo.Mode()&os.ModeCharDevice != 0 {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		}
	}
	config, ns := ParseConfiguration()

	ctx, cancel := context.WithCancel(context.Background())
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		cancel()
	}()

	err = NewDaemon(config, ns).Run(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to run daemon")
	}
}
