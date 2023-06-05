package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"math/rand"
	"sync"
	"time"
)

const DatabaseOpenInterval = 5 * time.Second

const PeriodicArpInterval = 10 * time.Second
const NumConsistentHashingUids = 100

type Configuration struct {
	Node             string
	Vnis             []uint64
	MigrationTimeout time.Duration
	ScanInterval     time.Duration
	DrainOnShutdown  bool
	Report           bool
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
	periodicArpChan      chan bool
	uids                 []uint64
	log                  zerolog.Logger
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
	uids := make([]uint64, NumConsistentHashingUids)
	for i := range uids {
		uids[i] = rand.Uint64()
	}
	return &Daemon{
		Config:               config,
		assignmentStrategy:   as,
		networkStrategy:      ns,
		vniEventIngestor:     VniEventIngestor{},
		timerEventIngestor:   NewTimerEventIngestor(),
		newNodeEventIngestor: NewNodeEventIngestor{},
		leaderEventIngestor:  LeaderEventIngestor{},
		eventProcessor:       DefaultEventProcessor{},
		uids:                 uids,
		log:                  log.With().Str("node", config.Node).Logger(),
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

func (d *Daemon) StartPeriodicArp() {
	d.periodicArpChan <- true
}

func (d *Daemon) StopPeriodicArp() {
	d.periodicArpChan <- false
}

func (d *Daemon) InitPeriodicArp(ctx context.Context) {
	d.periodicArpChan = make(chan bool, 1)

	go func() {
		enabled := <-d.periodicArpChan
		for {
			select {
			case <-ctx.Done():
				return
			case enabled = <-d.periodicArpChan:
				continue
			case <-time.After(PeriodicArpInterval):
				if enabled {
					state, err := d.db.GetFullState(d.Config, -1)
					if err != nil {
						d.log.Error().Err(err).Msg("periodic arp: failed to get full state")
						continue
					}
					for vni, vniState := range state {
						if vniState.Type == Idle && vniState.Current == d.Config.Node {
							err = d.networkStrategy.SendGratuitousArp(vni)
							if err != nil {
								d.log.Error().Err(err).Msg("periodic arp: failed to send gratuitous arp")
							}
						}
					}
				}
			}
		}
	}()
}

type DaemonState int

const (
	DatabaseConnected DaemonState = iota
	DatabaseDisconnected
	Drain
	Exit
)

func (d *Daemon) SetupDatabase(ctx context.Context, cancelDaemon context.CancelFunc, drainCtx context.Context) (<-chan v3.WatchChan, <-chan v3.WatchChan, error) {
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
		d.StartPeriodicArp()
		vniChanChan <- d.db.client.Watch(context.Background(), EtcdVniPrefix, v3.WithPrefix(), v3.WithCreatedNotify())
		nodeChanChan <- d.db.client.Watch(context.Background(), EtcdNodePrefix, v3.WithPrefix(), v3.WithCreatedNotify())
		err = d.db.Register(d.Config.Node, d.uids)
		if err != nil {
			d.log.Error().Err(err).Msg("failed to register node")
		}
		wg.Done()

		ticker := time.NewTicker(DatabaseOpenInterval)
		status := DatabaseConnected
		statusUpdateChan := make(chan DaemonState, 1)
		statusUpdateChan <- status

		drainChan := drainCtx.Done()
		exitChan := ctx.Done()

		for {
			select {
			case <-exitChan:
				statusUpdateChan <- Exit
				exitChan = nil
			case <-drainChan:
				statusUpdateChan <- Drain
				drainChan = nil
			case status = <-statusUpdateChan:
				switch status {
				case DatabaseConnected:
					d.StartPeriodicArp()
					vniChanChan <- d.db.client.Watch(context.Background(), EtcdVniPrefix, v3.WithPrefix(), v3.WithCreatedNotify())
					nodeChanChan <- d.db.client.Watch(context.Background(), EtcdNodePrefix, v3.WithPrefix(), v3.WithCreatedNotify())
					err = d.db.Register(d.Config.Node, d.uids)
					if err != nil {
						d.log.Error().Err(err).Msg("failed to register node, DatabaseDisconnected")
						statusUpdateChan <- DatabaseDisconnected
					}
				case DatabaseDisconnected:
					d.log.Info().Msg("database keepalive channel closed, withdrawing all and reopening database")
					d.StopPeriodicArp()
					respChan = nil
					cancel()
					err = d.WithdrawAll()
					if err != nil {
						d.log.Error().Err(err).Msg("failed to withdraw all on keepalive channel close")
					}
				case Drain:
					d.log.Info().Msg("drain requested, unregistering node")
					err = d.db.Unregister(d.Config.Node)
					if err != nil {
						d.log.Error().Err(err).Msg("failed to unregister node, Exit")
						statusUpdateChan <- Exit
					}
					nodes, err := d.db.Nodes()
					if err != nil {
						d.log.Error().Err(err).Msg("failed to get nodes, Exit")
						statusUpdateChan <- Exit
					}
					if len(nodes) == 0 {
						d.log.Info().Msg("no nodes left, exiting")
						statusUpdateChan <- Exit
					}
				case Exit:
					cancel()
					d.StopPeriodicArp()
					ticker.Stop()
					cancelDaemon()
					return
				}
			case _, ok := <-respChan:
				if !ok {
					statusUpdateChan <- DatabaseDisconnected
				}
			case <-ticker.C:
				if status == DatabaseDisconnected {
					newRespChan, newCancel, err := d.db.CreateLeaseAndKeepalive(ctx)
					if err != nil {
						continue
					}
					respChan = newRespChan
					cancel = newCancel
					statusUpdateChan <- DatabaseConnected
				} else if status == Drain {
					dbState, err := d.db.GetFullState(d.Config, -1)
					if err != nil {
						d.log.Error().Err(err).Msg("failed to get full state, exiting")
						statusUpdateChan <- Exit
					}
					found := false
					for _, vniState := range dbState {
						if vniState.Current == d.Config.Node {
							found = true
							break
						}
					}
					if !found {
						d.log.Info().Msg("drain complete, exiting")
						statusUpdateChan <- Exit
					}
				}
			}
		}
	}()
	wg.Wait()
	return vniChanChan, nodeChanChan, nil
}

func (d *Daemon) Run(drainCtx context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d.InitPeriodicArp(ctx)
	var err error

	d.vniEventIngestor.WatchChanChan, d.newNodeEventIngestor.WatchChanChan, err = d.SetupDatabase(ctx, cancel, drainCtx)
	if err != nil {
		return errors.Wrap(err, "failed to setup database")
	}
	defer d.db.Close()
	err = d.WithdrawAll()
	if err != nil {
		return errors.Wrap(err, "failed to withdraw all on startup")
	}

	leaderChan := make(chan LeaderState, 1)
	vniChan := make(chan VniEvent, 1)
	newNodeChan := make(chan NewNodeEvent, 1)
	timerChan := make(chan TimerEvent, 1)

	wg := new(sync.WaitGroup)
	runEventIngestor[VniEvent](ctx, d, d.vniEventIngestor, vniChan, wg)
	runEventIngestor[TimerEvent](ctx, d, d.timerEventIngestor, timerChan, wg)
	runEventIngestor[NewNodeEvent](ctx, d, d.newNodeEventIngestor, newNodeChan, wg)
	runEventIngestor[LeaderState](ctx, d, d.leaderEventIngestor, leaderChan, wg)

	wg.Add(1)
	go func() {
		err := d.eventProcessor.Process(ctx, d, vniChan, leaderChan, newNodeChan, timerChan)
		if err != nil {
			d.log.Fatal().Err(err).Msg("failed to process events")
		}
		wg.Done()
	}()

	if d.Config.Report {
		wg.Add(1)
		go func() {
			NewReporter().Start(ctx, d)
			wg.Done()
		}()
	}

	<-drainCtx.Done()
	if !d.Config.DrainOnShutdown {
		cancel()
	}
	<-ctx.Done()

	wg.Wait()
	d.log.Debug().Msg("exiting, withdrawing everything")
	err = d.WithdrawAll()
	return errors.Wrap(err, "failed to withdraw all on shutdown")
}
