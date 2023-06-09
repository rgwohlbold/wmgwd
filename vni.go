package main

import (
	"context"
	"github.com/emirpasic/gods/sets/treeset"
	"github.com/emirpasic/gods/utils"
	v3 "go.etcd.io/etcd/client/v3"
)

type VniState struct {
	Type     VniStateType
	Current  string
	Next     string
	Revision int64
}

type VniEvent struct {
	State VniState
	Vni   uint64
}

type VniEventIngestor struct {
	WatchChanChan <-chan v3.WatchChan
	Daemon        *Daemon
}

func NewVniEventIngestor(d *Daemon) VniEventIngestor {
	return VniEventIngestor{
		Daemon: d,
	}
}

func (i VniEventIngestor) Ingest(ctx context.Context, ch chan<- VniEvent, setupChan chan<- struct{}) {
	d := i.Daemon
	watchChan := d.db.client.Watch(ctx, EtcdVniPrefix, v3.WithPrefix(), v3.WithCreatedNotify())
	setupChan <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			d.log.Debug().Msg("vni-watcher: context done")
			return
		case e, ok := <-watchChan:
			if !ok {
				watchChan = <-i.WatchChanChan
				continue
			}
			vnis := treeset.NewWith(utils.UInt64Comparator)
			revision := e.Header.Revision
			for _, ev := range e.Events {
				parsedVni, err := d.db.VniFromKv(ev.Kv)
				if err != nil {
					d.log.Error().Err(err).Msg("vni-watcher: failed to parse vni")
					continue
				}
				vnis.Add(parsedVni)
			}
			if vnis.Size() == 0 {
				continue
			}
			for _, vni := range vnis.Values() {
				vni := vni.(uint64)
				state, err := d.db.GetState(vni, revision) // TODO: this may block for a long time
				if err != nil {
					d.log.Error().Err(err).Msg("vni-watcher: failed to parse vni state")
					continue
				}
				if len(ch) == cap(ch) {
					d.log.Warn().Uint64("vni", vni).Msg("vni-watcher: channel full")
				}
				ch <- VniEvent{Vni: vni, State: state}
			}
		}
	}
}
