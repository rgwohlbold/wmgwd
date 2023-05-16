package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

type Configuration struct {
	Node             string
	Vnis             []uint64
	MigrationTimeout time.Duration
}

type Daemon struct {
	Config               Configuration
	networkStrategy      NetworkStrategy
	vniEventIngestor     VniEventIngestor
	timerEventIngestor   TimerEventIngestor
	newNodeEventIngestor NewNodeEventIngestor
	leaderEventIngestor  LeaderEventIngestor
	eventProcessor       EventProcessor
}

func NewDaemon(config Configuration, networkStrategy NetworkStrategy) *Daemon {
	return &Daemon{
		Config:               config,
		networkStrategy:      networkStrategy,
		vniEventIngestor:     VniEventIngestor{},
		timerEventIngestor:   NewTimerEventIngestor(),
		newNodeEventIngestor: NewNodeEventIngestor{},
		leaderEventIngestor:  LeaderEventIngestor{},
		eventProcessor:       DefaultEventProcessor{},
	}
}

func (d *Daemon) Register() error {
	db, err := NewDatabase(context.Background(), d.Config)
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
		err := d.eventProcessor.Process(ctx, d, vniChan, leaderChan, newNodeChan, timerChan)
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
