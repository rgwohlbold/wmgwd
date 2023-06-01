package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"time"
)

const ReportInterval = 5 * time.Second

type Reporter struct {
}

func NewReporter() *Reporter {
	return &Reporter{}
}

func (r *Reporter) Report(d *Daemon) error {
	states, err := d.db.GetFullState(d.Config, -1)
	if err != nil {
		return err
	}
	for vni, state := range states {
		if state.Type != Idle || state.Current != d.Config.Node {
			continue
		}
		var newReport uint64
		newReport, err = d.networkStrategy.ByteCounter(vni)
		if err != nil {
			log.Error().Err(err).Msg("reporter: failed to get byte counter")
			continue
		}
		err = d.db.NewVniUpdate(vni).Revision(state.Revision).Report(newReport).Run()
		if err != nil {
			log.Error().Err(err).Msg("reporter: failed to update report")
			continue
		}
	}
	return nil
}

func (r *Reporter) Start(ctx context.Context, d *Daemon) {
	ticker := time.NewTicker(ReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Debug().Msg("reporter: reporting")
			err := r.Report(d)
			if err != nil {
				log.Error().Err(err).Msg("reporter: failed to report")
			}
		}
	}
}
