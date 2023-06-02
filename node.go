package main

import (
	"context"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
)

type NewNodeEventIngestor struct {
	WatchChanChan <-chan v3.WatchChan
}

type NewNodeEvent struct {
	Node string
}

func (i NewNodeEventIngestor) Ingest(ctx context.Context, d *Daemon, newNodeChan chan<- NewNodeEvent) {
	watchChan := <-i.WatchChanChan
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("node-watcher: context done")
			return
		case e, ok := <-watchChan:
			if !ok {
				watchChan = <-i.WatchChanChan
				continue
			}
			for _, ev := range e.Events {
				if ev.Type != v3.EventTypePut {
					continue
				}
				newNodeChan <- NewNodeEvent{Node: string(ev.Kv.Value)}
			}
		}
	}
}
