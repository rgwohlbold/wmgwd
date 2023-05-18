package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type LeaderState struct {
	Node string
	Key  string
}

type LeaderEventIngestor struct{}

func (_ LeaderEventIngestor) Ingest(ctx context.Context, d *Daemon, leaderChan chan<- LeaderState, setupChan chan<- struct{}) {
	// we will never miss a relevant leader event: we are followers first and always observe ourselves being elected
	setupChan <- struct{}{}

	db, err := NewDatabase(ctx, d.Config)
	if err != nil {
		log.Fatal().Err(err).Msg("leader-election: failed to connect to database")
	}
	defer db.Close()

	leaderChan <- LeaderState{Node: "", Key: ""}
	session, err := concurrency.NewSession(db.client, concurrency.WithTTL(EtcdLeaseTTL))
	if err != nil {
		log.Fatal().Err(err).Msg("leader-election: failed to create session")
	}
	defer func(session *concurrency.Session) {
		err = session.Close()
		if err != nil {
			log.Error().Err(err).Msg("leader-election: failed to close session")
		}
	}(session)

	// strip slash since it is added by the concurrency library
	electionKey := EtcdLeaderPrefix[:len(EtcdLeaderPrefix)-1]
	election := concurrency.NewElection(session, electionKey)
	for {
		log.Info().Msg("leader-election: campaigning")
		err = election.Campaign(ctx, d.Config.Node)
		if err != nil {
			// we do not use an own context, so all context errors termination signals
			if err == context.Canceled || err == context.DeadlineExceeded {
				goto end
			}
			log.Fatal().Err(err).Msg("leader-election: campaign failed")
		}
		log.Info().Str("key", election.Key()).Msg("leader-election: got elected")
		leaderChan <- LeaderState{Node: d.Config.Node, Key: election.Key()}
		observeChan := election.Observe(ctx)
		for {
			select {
			case value := <-observeChan:
				if string(value.Kvs[0].Value) == d.Config.Node {
					continue
				}
				log.Info().Msg("leader-election: lost election")
				leaderChan <- LeaderState{Node: d.Config.Node, Key: election.Key()}
			case <-ctx.Done():
				goto end
			}
		}
	}
end:
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	log.Debug().Msg("leader-election: context done")
	err = election.Resign(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("leader-election: failed to resign")
	}
	return
}
