package main

import (
	"context"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
)

type VniState struct {
	Type    VniStateType
	Current string
	Next    string
	Counter int
}

type VniEvent struct {
	State VniState
	Vni   uint64
}

type VniEventIngestor struct{}

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
			revision := int64(0)
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

				parsedRevision := ev.Kv.ModRevision
				if revision != 0 && parsedRevision != revision {
					log.Error().Int64("revision", revision).Int64("parsed-revision", parsedRevision).Msg("vni-watcher: got state for multiple revisions")
					continue
				}
				revision = parsedRevision
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
