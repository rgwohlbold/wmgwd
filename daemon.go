package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
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
	Ingest(ctx context.Context, daemon *Daemon, eventChan chan<- E)
}

func runEventIngestor[E any](ctx context.Context, daemon *Daemon, ingestor EventIngestor[E], eventChan chan<- E, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		ingestor.Ingest(ctx, daemon, eventChan)
		wg.Done()
	}()
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

func (d *Daemon) SetupDatabase(ctx context.Context) (<-chan v3.WatchChan, <-chan v3.WatchChan, error) {
	vniChanChan := make(chan v3.WatchChan, 1)
	nodeChanChan := make(chan v3.WatchChan, 1)
	var err error
	d.db, err = NewDatabase(d.Config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create database")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		var respChan <-chan *v3.LeaseKeepAliveResponse
		var cancel context.CancelFunc
		for {
			respChan, cancel, err = d.db.CreateLeaseAndKeepalive(ctx)
			if err == nil {
				break
			}
		}
		vniChanChan <- d.db.client.Watch(context.Background(), EtcdVniPrefix, v3.WithPrefix(), v3.WithCreatedNotify())
		nodeChanChan <- d.db.client.Watch(context.Background(), EtcdNodePrefix, v3.WithPrefix(), v3.WithCreatedNotify())
		err = d.db.Register(d.Config.Node)
		if err != nil {
			log.Error().Err(err).Msg("failed to register node")
		}
		wg.Done()

		var ticker *time.Ticker
		var tickerChan <-chan time.Time = nil
		for {
			select {
			case <-ctx.Done():
				if cancel != nil {
					cancel()
				}
				d.db.Close()
				return
			case _, ok := <-respChan:
				if !ok {
					log.Info().Msg("database keepalive channel closed, withdrawing all and reopening database")
					respChan = nil
					cancel()
					err = d.WithdrawAll()
					if err != nil {
						log.Error().Err(err).Msg("failed to withdraw all on keepalive channel close")
					}
					ticker = time.NewTicker(DatabaseOpenInterval)
					tickerChan = ticker.C
				}
			case <-tickerChan:
				newRespChan, newCancel, err := d.db.CreateLeaseAndKeepalive(ctx)
				if err != nil {
					continue
				}
				log.Info().Msg("database reopened")
				respChan = newRespChan
				cancel = newCancel
				ticker.Stop()
				ticker = nil

				vniChanChan <- d.db.client.Watch(context.Background(), EtcdVniPrefix, v3.WithPrefix(), v3.WithCreatedNotify())
				nodeChanChan <- d.db.client.Watch(context.Background(), EtcdNodePrefix, v3.WithPrefix(), v3.WithCreatedNotify())
				err = d.db.Register(d.Config.Node)
				if err != nil {
					log.Error().Err(err).Msg("failed to register node")
				}
			}
		}
	}()
	wg.Wait()
	return vniChanChan, nodeChanChan, nil
}

func (d *Daemon) Run(ctx context.Context) error {
	var err error
	d.vniEventIngestor.WatchChanChan, d.newNodeEventIngestor.WatchChanChan, err = d.SetupDatabase(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to setup database")
	}
	err = d.WithdrawAll()
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
