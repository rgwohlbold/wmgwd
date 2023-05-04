package main

import (
	"context"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
)

type NewNodeEvent struct {
	Node string
}

func GenerateNewNodeEvents(ctx context.Context, newNodeChan chan<- NewNodeEvent) {
	client, err := v3.New(v3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to etcd")
	}
	defer client.Close()

	watchChan := client.Watch(ctx, EtcdNodePrefix, v3.WithPrefix())

	for {
		e := <-watchChan
		for _, ev := range e.Events {
			if ev.Type != v3.EventTypePut {
				continue
			}
			newNodeChan <- NewNodeEvent{Node: string(ev.Kv.Value)}
		}
	}
}
