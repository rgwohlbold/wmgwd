package main

import (
	"context"
	v3 "go.etcd.io/etcd/client/v3"
)

type VniState struct {
	Type     VniStateType
	Current  string
	Next     string
	Revision int64
	Report   uint64
}

type VniEvent struct {
	State VniState
	Vni   uint64
}

type VniEventIngestor struct {
	WatchChanChan <-chan v3.WatchChan
}

const InvalidVni = ^uint64(0)

func (i VniEventIngestor) Ingest(ctx context.Context, d *Daemon, ch chan<- VniEvent) {
	watchChan := <-i.WatchChanChan
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
			vni := InvalidVni
			revision := e.Header.Revision
			for _, ev := range e.Events {
				parsedVni, err := d.db.VniFromKv(ev.Kv)
				if err != nil {
					d.log.Error().Err(err).Msg("vni-watcher: failed to parse vni")
					continue
				}
				if vni != InvalidVni && parsedVni != vni {
					d.log.Error().Uint64("vni", vni).Uint64("parsed-vni", parsedVni).Msg("vni-watcher: got state for multiple vnis")
					continue
				}
				vni = parsedVni
			}
			if vni == InvalidVni {
				d.log.Error().Msg("vni-watcher: got event for no vnis")
				continue
			}

			state, err := d.db.GetState(vni, revision)
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
