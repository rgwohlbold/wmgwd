package main

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"strings"
)

type VNIState struct {
	Type    VNIStateType `json:"type"`
	Current string       `json:"current"`
	Next    string       `json:"next"`
}

type VNIEvent struct {
	State VNIState
	VNI   int
}

type VNIEventIngestor struct{}

func (_ VNIEventIngestor) Ingest(ctx context.Context, node string, ch chan<- VNIEvent, setupChan chan<- struct{}) {
	db, err := NewDatabase(node)
	if err != nil {
		log.Fatal().Err(err).Msg("vni-watcher: failed to connect to database")
	}
	defer db.Close()

	watchChan := db.client.Watch(context.Background(), EtcdVNIPrefix, v3.WithPrefix())
	setupChan <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("vni-watcher: context done")
			return
		case e := <-watchChan:
			for _, ev := range e.Events {
				keyRest := strings.TrimPrefix(string(ev.Kv.Key), EtcdVNIPrefix)
				vni, err := strconv.Atoi(keyRest)
				if err != nil {
					log.Error().Str("key", string(ev.Kv.Key)).Str("vni", keyRest).Err(err).Msg("vni-watcher: failed to parse vni")
					continue
				}
				state := VNIState{}

				if ev.Type == v3.EventTypeDelete {
					state.Type = Unassigned
				} else {
					err = json.Unmarshal(ev.Kv.Value, &state)
					if err != nil {
						log.Error().Str("key", string(ev.Kv.Key)).Str("value", string(ev.Kv.Value)).Err(err).Msg("vni-watcher: failed to parse state")
						continue
					}
				}

				ch <- VNIEvent{VNI: vni, State: state}
			}
		}
	}
}
