package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"sync"
	"time"
)

const DatabaseOpenInterval = 5 * time.Second

const PeriodicArpInterval = 10 * time.Second
const NumConsistentHashingUids = 100

type Configuration struct {
	Name             string
	Uids             []uint64
	Vnis             []uint64
	MigrationTimeout time.Duration
	FailoverTimeout  time.Duration
	ScanInterval     time.Duration
	DrainOnShutdown  bool
}

type Daemon struct {
	Config            Configuration
	networkStrategy   NetworkStrategy
	vniEventIngestor  VniEventIngestor
	nodeEventIngestor NodeEventIngestor
	eventProcessor    EventProcessor
	db                *Database
	periodicArpChan   chan bool
	log               zerolog.Logger
}

type EventIngestor[E any] interface {
	Ingest(ctx context.Context, eventChan chan<- E, setupChan chan<- struct{})
}

func runEventIngestor[E any](ctx context.Context, ingestor EventIngestor[E], eventChan chan<- E, wg *sync.WaitGroup) {
	wg.Add(1)
	setupChan := make(chan struct{})
	go func() {
		ingestor.Ingest(ctx, eventChan, setupChan)
		wg.Done()
	}()
	<-setupChan
}

func NewDaemon(config Configuration, ns NetworkStrategy) *Daemon {
	daemon := &Daemon{
		Config:          config,
		networkStrategy: ns,
		log:             log.With().Str("node", config.Name).Logger(),
	}
	daemon.eventProcessor = NewDefaultEventProcessor(daemon)
	daemon.vniEventIngestor = NewVniEventIngestor(daemon)
	daemon.nodeEventIngestor = NewNodeEventIngestor(daemon)
	return daemon
}

func (d *Daemon) WithdrawAll() error {
	wg := sync.WaitGroup{}
	for _, vni := range d.Config.Vnis {
		wg.Add(3)
		go func(vni uint64) {
			defer wg.Done()
			err := d.networkStrategy.WithdrawEvpn(vni)
			if err != nil {
				log.Error().Err(err).Uint64("vni", vni).Msg("failed to withdraw evpn")
			}
		}(vni)
		go func(vni uint64) {
			defer wg.Done()
			err := d.networkStrategy.AdvertiseOspf(vni, OspfWithdrawCost)
			if err != nil {
				log.Error().Err(err).Uint64("vni", vni).Msg("failed to withdraw ospf")
			}
		}(vni)
		go func(vni uint64) {
			defer wg.Done()
			err := d.networkStrategy.DisableArp(vni)
			if err != nil {
				log.Error().Err(err).Uint64("vni", vni).Msg("failed to disable arp")
			}
		}(vni)
	}
	wg.Wait()
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
					state, err := d.db.GetFullState(ctx, d.Config, -1)
					if err != nil {
						d.log.Error().Err(err).Msg("periodic arp: failed to get full state")
						continue
					}
					for vni, vniState := range state {
						if vniState.Type == Idle && vniState.Current == d.Config.Name {
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

func (d *Daemon) SetupDatabase(ctx context.Context, cancelDaemon context.CancelFunc, drainCtx context.Context, wg *sync.WaitGroup) error {
	var err error
	d.db, err = NewDatabase(d.Config)
	if err != nil {
		return errors.Wrap(err, "failed to create database")
	}

	var innerWg sync.WaitGroup
	innerWg.Add(1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var respChan <-chan *v3.LeaseKeepAliveResponse
		var cancel context.CancelFunc
		for {
			respChan, cancel, err = d.db.CreateLeaseAndKeepalive(ctx)
			if err == nil {
				break
			} else {
				d.log.Error().Err(err).Msg("failed to create lease and keepalive")
			}
		}
		d.db.pool.Start(ctx)
		d.StartPeriodicArp()
		err = d.db.Register(d.Config.Name, d.Config.Uids)
		if err != nil {
			d.log.Error().Err(err).Msg("failed to register node")
		}
		innerWg.Done()

		ticker := time.NewTicker(DatabaseOpenInterval)
		status := DatabaseConnected

		drainChan := drainCtx.Done()
		exitChan := ctx.Done()

		done := false

		var transition func(newStatus DaemonState)
		t := func(newStatus DaemonState) {
			if newStatus == status {
				return
			}
			status = newStatus
			if status == DatabaseConnected {
				d.StartPeriodicArp()
				err = d.db.Register(d.Config.Name, d.Config.Uids)
				if err != nil {
					d.log.Error().Err(err).Msg("failed to register node, DatabaseDisconnected")
					transition(DatabaseDisconnected)
					return
				}
			} else if status == DatabaseDisconnected {
				d.StopPeriodicArp()
				respChan = nil
				cancel()
				err = d.WithdrawAll()
				if err != nil {
					d.log.Error().Err(err).Msg("failed to withdraw all on keepalive channel close")
				}
			} else if status == Drain {
				d.log.Info().Msg("drain requested, unregistering node")
				err = d.db.Unregister(d.Config.Name)
				if err != nil {
					d.log.Error().Err(err).Msg("failed to unregister node, Exit")
					transition(Exit)
					return
				}
				nodes, err := d.db.Nodes(ctx)
				if err != nil {
					d.log.Error().Err(err).Msg("failed to get nodes, Exit")
					transition(Exit)
					return
				}
				if len(nodes) == 0 {
					d.log.Info().Msg("no nodes left, exiting")
					transition(Exit)
					return
				}
			} else if status == Exit {
				err = d.db.Unregister(d.Config.Name)
				if err != nil {
					d.log.Error().Err(err).Msg("failed to unregister node on exit")
				}
				cancel()
				d.StopPeriodicArp()
				ticker.Stop()
				cancelDaemon()
				done = true
			}
		}
		transition = t

		for !done {
			select {
			case <-exitChan:
				transition(Exit)
				exitChan = nil
			case <-drainChan:
				transition(Drain)
				drainChan = nil
			case _, ok := <-respChan:
				if !ok {
					d.log.Info().Msg("database keepalive channel closed")
					transition(DatabaseDisconnected)
				}
			case <-ticker.C:
				if status == DatabaseConnected {
					var nodes []Node
					nodes, err = d.db.Nodes(ctx)
					if err != nil {
						d.log.Error().Err(err).Msg("failed to get nodes")
						continue
					}
					found := false
					for _, node := range nodes {
						if node.Name == d.Config.Name {
							found = true
							break
						}
					}
					if !found {
						err = d.db.Register(d.Config.Name, d.Config.Uids)
						if err != nil {
							d.log.Error().Err(err).Msg("failed to register node")
						}
					}
				} else if status == DatabaseDisconnected {
					newRespChan, newCancel, err := d.db.CreateLeaseAndKeepalive(ctx)
					if err != nil {
						log.Error().Err(err).Msg("failed to create lease and keepalive")
						continue
					}
					respChan = newRespChan
					cancel = newCancel
					transition(DatabaseConnected)
				} else if status == Drain {
					dbState, err := d.db.GetFullState(ctx, d.Config, -1)
					if err != nil {
						d.log.Error().Err(err).Msg("failed to get full state, exiting")
						transition(Exit)
					}
					found := false
					for _, vniState := range dbState {
						if vniState.Current == d.Config.Name {
							found = true
							break
						}
					}
					if !found {
						d.log.Info().Msg("drain complete, exiting")
						transition(Exit)
					}
				}
			}
		}
	}()
	return nil
}

func (d *Daemon) Run(drainCtx context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch d.networkStrategy.(type) {
	case *SystemNetworkStrategy:
		go d.networkStrategy.(*SystemNetworkStrategy).Loop(context.Background())
	}

	d.InitPeriodicArp(ctx)
	wg := &sync.WaitGroup{}

	err := d.SetupDatabase(ctx, cancel, drainCtx, wg)
	if err != nil {
		return errors.Wrap(err, "failed to setup database")
	}
	defer d.db.Close()
	err = d.WithdrawAll()
	if err != nil {
		return errors.Wrap(err, "failed to withdraw all on startup")
	}

	vniChan := make(chan VniEvent, 1)
	newNodeChan := make(chan NodeEvent, 1)

	runEventIngestor[VniEvent](ctx, d.vniEventIngestor, vniChan, wg)
	runEventIngestor[NodeEvent](ctx, d.nodeEventIngestor, newNodeChan, wg)

	wg.Add(1)
	go func() {
		err := d.eventProcessor.Process(ctx, vniChan, newNodeChan)
		if err != nil {
			d.log.Fatal().Err(err).Msg("failed to process events")
		}
		wg.Done()
	}()

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
