package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"time"
)

const ReportInterval = 5 * time.Second

type Reporter struct {
	daemon *Daemon
}

func NewReporter(daemon *Daemon) *Reporter {
	return &Reporter{daemon}
}

func (r *Reporter) Report(ctx context.Context) error {
	states, err := r.daemon.db.GetFullState(ctx, r.daemon.Config, -1)
	if err != nil {
		return err
	}
	for vni, state := range states {
		if state.Type != Idle || state.Current != r.daemon.Config.Node {
			continue
		}
		var newReport uint64
		newReport, err = r.daemon.networkStrategy.ByteCounter(vni)
		if err != nil {
			r.daemon.log.Error().Err(err).Msg("reporter: failed to get byte counter")
			continue
		}
		err = r.daemon.db.NewVniUpdate(vni).Revision(state.Revision).Report(newReport).RunOnce(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Reporter) Start(ctx context.Context) {
	ticker := time.NewTicker(ReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := r.Report(ctx)
			if err != nil {
				log.Error().Err(err).Msg("reporter: failed to report")
			}
		}
	}
}
