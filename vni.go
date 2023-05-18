package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
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
			kvs := make([]*mvccpb.KeyValue, 0)
			for _, ev := range e.Events {
				if ev.Type == v3.EventTypePut {
					kvs = append(kvs, ev.Kv)
				}
			}
			if len(kvs) == 0 {
				continue
			}
			event, err := d.db.VniEventFromKvs(kvs, InvalidVni)
			if err != nil {
				log.Error().Err(err).Msg("vni-watcher: failed to parse vni event")
				continue
			}
			log.Debug().Interface("event", event).Msg("vni-watcher: got event")
			ch <- event
		}
	}
}
