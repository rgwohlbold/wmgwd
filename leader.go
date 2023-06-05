package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/client/v3/concurrency"
	"time"
)

type LeaderState struct {
	Node string
	Key  string
}

type LeaderEventIngestor struct{}

const LeaderTickerInterval = 1 * time.Second

func reliably(ctx context.Context, label string, fn func() error) {
	for {
		err := fn()
		if err == nil {
			return
		}
		log.Error().Err(err).Str("label", label).Msg("reliably: failed")
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (_ LeaderEventIngestor) Ingest(ctx context.Context, d *Daemon, leaderChan chan<- LeaderState) {
	leaderChan <- LeaderState{Node: "", Key: ""}

	// strip slash since it is added by the concurrency library
	electionKey := EtcdLeaderPrefix[:len(EtcdLeaderPrefix)-1]

	var session *concurrency.Session
	reliably(ctx, "leader-election: session", func() error {
		var err error
		session, err = concurrency.NewSession(d.db.client, concurrency.WithTTL(EtcdLeaseTTL))
		return err
	})
	election := concurrency.NewElection(session, electionKey)

	defer func(session *concurrency.Session) {
		err := session.Close()
		if err != nil {
			log.Error().Err(err).Msg("leader-election: failed to close session")
		}
	}(session)

	// campaign forever
	for {
	campaign:
		reliably(ctx, "leader-election: campaign", func() error {
			err := election.Campaign(ctx, d.Config.Node)
			if err != nil {
				err = session.Close()
				if err != nil {
					log.Error().Err(err).Msg("leader-election: failed to close session")
				}
				newSession, err := concurrency.NewSession(d.db.client, concurrency.WithTTL(EtcdLeaseTTL))
				if err != nil {
					log.Error().Err(err).Msg("leader-election: failed to create new session")
				} else {
					session = newSession
				}
				election = concurrency.NewElection(session, electionKey)
			}
			return err
		})
		leaderChan <- LeaderState{Node: d.Config.Node, Key: election.Key()}
		log.Info().Str("key", election.Key()).Msg("leader-election: got elected")
		for {
			select {
			case <-ctx.Done():
				goto end
			case <-time.After(LeaderTickerInterval):
				resp, err := election.Leader(ctx)
				if err == concurrency.ErrElectionNoLeader {
					log.Info().Msg("leader-election: no leader")
					goto campaign
				} else if err == context.Canceled {
					goto end
				} else if err != nil {
					log.Error().Err(err).Msg("leader-election: failed to get leader")
					goto campaign
				}
				leader := string(resp.Kvs[0].Value)
				if leader != d.Config.Node {
					leaderChan <- LeaderState{Node: string(resp.Kvs[0].Value), Key: election.Key()}
				}
			}
		}
	}
end:
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	log.Debug().Msg("leader-election: context done")
	err := election.Resign(ctx)
	if err != nil {
		log.Error().Err(err).Msg("leader-election: failed to resign")
	}
}
