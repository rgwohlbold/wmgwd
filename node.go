package main

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"strings"
)

type NodeEventIngestor struct {
	Daemon *Daemon
}

type NodeEventType int

const (
	NodeAdded NodeEventType = iota
	NodeRemoved
)

type NodeEvent struct {
	Type NodeEventType
	Node Node
}

func NewNodeEventIngestor(d *Daemon) NodeEventIngestor {
	return NodeEventIngestor{
		Daemon: d,
	}
}

func (i NodeEventIngestor) Ingest(ctx context.Context, nodeChan chan<- NodeEvent, setupChan chan<- struct{}) {
	d := i.Daemon
	watchChan := d.db.client.Watch(ctx, EtcdNodePrefix, v3.WithPrefix(), v3.WithCreatedNotify())
	setupChan <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("node-watcher: context done")
			return
		case e, ok := <-watchChan:
			if !ok {
				continue
			}
			for _, ev := range e.Events {
				if ev.Type == v3.EventTypePut {
					var uids []uint64
					err := json.Unmarshal(ev.Kv.Value, &uids)
					if err != nil {
						log.Error().Err(err).Msg("node-watcher: failed to unmarshal uids")
						continue
					}
					name := strings.TrimPrefix(string(ev.Kv.Key), EtcdNodePrefix)
					//log.Info().Str("name", string(ev.Kv.Key)).Msg("node-watcher: node added")
					nodeChan <- NodeEvent{
						Type: NodeAdded,
						Node: Node{
							Name:  name,
							Lease: v3.LeaseID(ev.Kv.Lease),
							Uids:  uids,
						}}
				} else if ev.Type == v3.EventTypeDelete {
					nodeChan <- NodeEvent{
						Type: NodeRemoved,
					}
				}
			}
		}
	}
}
