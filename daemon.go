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
	ScanInterval     time.Duration
}

type Daemon struct {
	Config               Configuration
	assignmentStrategy   AssignmentStrategy
	networkStrategy      NetworkStrategy
	vniEventIngestor     VniEventIngestor
	timerEventIngestor   TimerEventIngestor
	newNodeEventIngestor NewNodeEventIngestor
	leaderEventIngestor  LeaderEventIngestor
	eventProcessor       EventProcessor
	db                   *Database
}

func NewDaemon(config Configuration, ns NetworkStrategy, as AssignmentStrategy) *Daemon {
	return &Daemon{
		Config:               config,
		assignmentStrategy:   as,
		networkStrategy:      ns,
		vniEventIngestor:     VniEventIngestor{},
		timerEventIngestor:   NewTimerEventIngestor(),
		newNodeEventIngestor: NewNodeEventIngestor{},
		leaderEventIngestor:  LeaderEventIngestor{},
		eventProcessor:       DefaultEventProcessor{},
	}
}

func (d *Daemon) Run(ctx context.Context) error {
	var err error
	d.db, err = NewDatabase(ctx, d.Config)
	defer d.db.Close()
	if err != nil {
		return errors.Wrap(err, "could not open database")
	}
	for _, vni := range d.Config.Vnis {
		err = d.networkStrategy.WithdrawEvpn(vni)
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

	err = d.db.Register(d.Config.Node)
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
		err = d.networkStrategy.DisableArp(vni)
		if err != nil {
			return errors.Wrap(err, "failed to disable arp")
		}
	}
	return nil
}
