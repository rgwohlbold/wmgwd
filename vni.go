package main

import (
	"context"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
)

type VniState struct {
	Type     VniStateType
	Current  string
	Next     string
	Revision int64
	Report   uint64
}

type VniEvent struct {
	State VniState
	Vni   uint64
}

type VniEventIngestor struct{}

const InvalidVni = ^uint64(0)

func (_ VniEventIngestor) Ingest(ctx context.Context, d *Daemon, ch chan<- VniEvent, setupChan chan<- struct{}) {
	watchChan := d.db.client.Watch(context.Background(), EtcdVniPrefix, v3.WithPrefix(), v3.WithCreatedNotify())
	setupChan <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("vni-watcher: context done")
			return
		case e := <-watchChan:
			vni := InvalidVni
			revision := e.Header.Revision
			for _, ev := range e.Events {
				parsedVni, err := d.db.VniFromKv(ev.Kv)
				if err != nil {
					log.Error().Err(err).Msg("vni-watcher: failed to parse vni")
					continue
				}
				if vni != InvalidVni && parsedVni != vni {
					log.Error().Uint64("vni", vni).Uint64("parsed-vni", parsedVni).Msg("vni-watcher: got state for multiple vnis")
					continue
				}
				vni = parsedVni
			}
			if vni == InvalidVni {
				log.Error().Msg("vni-watcher: got event for no vnis")
				continue
			}

			state, err := d.db.GetState(vni, revision)
			if err != nil {
				log.Error().Err(err).Msg("vni-watcher: failed to parse vni state")
				continue
			}
			log.Debug().Interface("state", state).Msg("vni-watcher: got state")
			ch <- VniEvent{Vni: vni, State: state}
		}
	}
}
