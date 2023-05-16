package main

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"strings"
)

type VniState struct {
	Type    VniStateType `json:"type"`
	Current string       `json:"current"`
	Next    string       `json:"next"`
}

type VniEvent struct {
	State VniState
	Vni   uint64
}

type VniEventIngestor struct{}

func (_ VniEventIngestor) Ingest(ctx context.Context, d *Daemon, ch chan<- VniEvent, setupChan chan<- struct{}) {
	db, err := NewDatabase(ctx, d.Config)
	if err != nil {
		log.Fatal().Err(err).Msg("vni-watcher: failed to connect to database")
	}
	defer db.Close()

	watchChan := db.client.Watch(context.Background(), EtcdVniPrefix, v3.WithPrefix())
	setupChan <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("vni-watcher: context done")
			return
		case e := <-watchChan:
			for _, ev := range e.Events {
				keyRest := strings.TrimPrefix(string(ev.Kv.Key), EtcdVniPrefix)
				vni, err := strconv.ParseUint(keyRest, 10, 32)
				if err != nil {
					log.Error().Str("key", string(ev.Kv.Key)).Str("vni", keyRest).Err(err).Msg("vni-watcher: failed to parse vni")
					continue
				}
				state := VniState{}

				if ev.Type == v3.EventTypeDelete {
					state.Type = Unassigned
				} else {
					err = json.Unmarshal(ev.Kv.Value, &state)
					if err != nil {
						log.Error().Str("key", string(ev.Kv.Key)).Str("value", string(ev.Kv.Value)).Err(err).Msg("vni-watcher: failed to parse state")
						continue
					}
				}

				ch <- VniEvent{Vni: vni, State: state}
			}
		}
	}
}
