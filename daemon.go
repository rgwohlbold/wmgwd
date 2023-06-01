package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
	"time"
)

const DatabaseOpenInterval = 5 * time.Second

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

type EventIngestor[E any] interface {
	Ingest(ctx context.Context, daemon *Daemon, eventChan chan<- E, setupChan chan<- struct{})
}

func runEventIngestor[E any](ctx context.Context, daemon *Daemon, ingestor EventIngestor[E], eventChan chan<- E, wg *sync.WaitGroup) {
	wg.Add(1)
	setupChan := make(chan struct{})
	go func() {
		ingestor.Ingest(ctx, daemon, eventChan, setupChan)
		wg.Done()
	}()
	<-setupChan
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

func (d *Daemon) WithdrawAll() error {
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
	return nil
}

func (d *Daemon) SetupDatabase(ctx context.Context) {
	var wg sync.WaitGroup
	firstSetup := false
	wg.Add(1)
	go func() {
		c := make(chan time.Time, 1)
		c <- time.Now()

		var ticker *time.Ticker
		var tickerChan <-chan time.Time = c
		var keepaliveChan <-chan *clientv3.LeaseKeepAliveResponse
		for {
			if ticker != nil {
				tickerChan = ticker.C
			}

			select {
			case <-ctx.Done():
				if d.db != nil {
					d.db.Close()
				}
				return
			case <-tickerChan:
				db, err := NewDatabase(ctx, d.Config)
				if err != nil {
					log.Error().Err(err).Msg("failed to reopen database")
				} else if err == nil {
					d.db = db
					keepaliveChan = d.db.keepaliveChan
					if ticker != nil {
						ticker.Stop()
						ticker = nil
					}
					if !firstSetup {
						wg.Done()
						firstSetup = true
					}
				}
			case _, ok := <-keepaliveChan:
				if !ok {
					keepaliveChan = nil
					log.Info().Msg("database keepalive channel closed, withdrawing all and reopening database")
					err := d.WithdrawAll()
					if err != nil {
						log.Error().Err(err).Msg("failed to withdraw all on keepalive channel close")
					}
					ticker = time.NewTicker(DatabaseOpenInterval)
					d.db.Close()

				}
			}
		}
	}()
	wg.Wait()
}

func (d *Daemon) Run(ctx context.Context) error {
	d.SetupDatabase(ctx)
	err := d.WithdrawAll()
	if err != nil {
		return errors.Wrap(err, "failed to withdraw all on startup")
	}

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
	wg.Add(1)
	go func() {
		NewReporter().Start(ctx, d)
		wg.Done()
	}()

	err = d.db.Register(d.Config.Node)
	if err != nil {
		return errors.Wrap(err, "could not register")
	}

	wg.Wait()
	log.Info().Msg("exiting, withdrawing everything")
	err = d.WithdrawAll()
	return errors.Wrap(err, "failed to withdraw all on shutdown")
}
