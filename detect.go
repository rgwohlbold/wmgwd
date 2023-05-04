package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
	"strings"
)

type NodeAction int

const (
	NodeAdded NodeAction = iota
	NodeFailed
)

type NodeEvent struct {
	Type NodeAction
	Node string
}

func ShowPresence(ctx context.Context, node string) {
	client, err := v3.New(v3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to etcd")
	}
	lease, err := createLease(ctx, client)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create lease")
	}
	_, err = client.Put(ctx, EtcdNodePrefix+node, node, v3.WithLease(lease.ID))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to put")
	}
	ch, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to keep alive")
	}
	for range ch {
		// wait until channel closes
	}
}

func DetectFailure(ch chan<- NodeEvent) {
	client, err := v3.New(v3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to etcd")
	}
	watchChan := client.Watch(context.Background(), EtcdNodePrefix, v3.WithPrefix())
	for {
		e := <-watchChan
		for _, ev := range e.Events {
			if ev.Type == mvccpb.DELETE {
				node := strings.TrimPrefix(string(ev.Kv.Key), EtcdNodePrefix)
				log.Info().Str("node", node).Msg("node failed")
				ch <- NodeEvent{Type: NodeFailed, Node: node}
			} else if ev.Type == mvccpb.PUT {
				node := strings.TrimPrefix(string(ev.Kv.Key), EtcdNodePrefix)
				log.Info().Str("node", node).Msg("node added")
				ch <- NodeEvent{Type: NodeAdded, Node: node}
			}
		}
	}
}
