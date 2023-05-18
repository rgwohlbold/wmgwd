package main

import (
	"context"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
)

type NewNodeEventIngestor struct{}

type NewNodeEvent struct {
	Node string
}

func (_ NewNodeEventIngestor) Ingest(ctx context.Context, d *Daemon, newNodeChan chan<- NewNodeEvent, setupChan chan<- struct{}) {
	db, err := NewDatabase(ctx, d.Config)
	if err != nil {
		log.Fatal().Err(err).Msg("node-watcher: failed to connect to database")
	}
	defer db.Close()

	watchChan := db.client.Watch(ctx, EtcdNodePrefix, v3.WithPrefix())
	setupChan <- struct{}{}
	for {
		e, ok := <-watchChan
		if !ok {
			log.Debug().Msg("node-watcher: context done")
			break
		}
		for _, ev := range e.Events {
			if ev.Type != v3.EventTypePut {
				continue
			}
			newNodeChan <- NewNodeEvent{Node: string(ev.Kv.Value)}
		}
	}
}
