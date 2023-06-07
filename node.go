package main

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"strings"
)

type NewNodeEventIngestor struct {
	WatchChanChan <-chan v3.WatchChan
}

type NewNodeEvent struct {
	Node Node
}

func (i NewNodeEventIngestor) Ingest(ctx context.Context, newNodeChan chan<- NewNodeEvent) {
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
				var uids []uint64
				err := json.Unmarshal(ev.Kv.Value, &uids)
				if err != nil {
					log.Error().Err(err).Msg("node-watcher: failed to unmarshal uids")
					continue
				}
				name := strings.TrimPrefix(string(ev.Kv.Key), EtcdNodePrefix)
				newNodeChan <- NewNodeEvent{Node: Node{
					Name:  name,
					Lease: v3.LeaseID(ev.Kv.Lease),
					Uids:  uids,
				}}
			}
		}
	}
}
