package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.etcd.io/etcd/client/v3/concurrency"
	"time"
)

type LeaderState struct {
	Node string
	Key  string
}

type LeaderEventIngestor struct {
	Daemon *Daemon
}

const LeaderTickerInterval = 1 * time.Second

func reliably(ctx context.Context, log zerolog.Logger, fn func() error) {
	for {
		err := fn()
		if err == nil {
			return
		}
		log.Error().Err(err).Msg("reliably: failed")
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func NewLeaderEventIngestor(daemon *Daemon) LeaderEventIngestor {
	return LeaderEventIngestor{Daemon: daemon}
}

func (i LeaderEventIngestor) Ingest(ctx context.Context, leaderChan chan<- LeaderState) {
	d := i.Daemon
	leaderChan <- LeaderState{Node: "", Key: ""}

	// strip slash since it is added by the concurrency library
	electionKey := EtcdLeaderPrefix[:len(EtcdLeaderPrefix)-1]

	var session *concurrency.Session
	reliably(ctx, d.log.With().Str("action", "leader-election: session").Logger(), func() error {
		var err error
		session, err = concurrency.NewSession(d.db.client, concurrency.WithTTL(EtcdLeaseTTL))
		return err
	})
	election := concurrency.NewElection(session, electionKey)

	defer func(session *concurrency.Session) {
		err := session.Close()
		if err != nil {
			d.log.Error().Err(err).Msg("leader-election: failed to close session")
		}
	}(session)

	// campaign forever
	for {
	campaign:
		reliably(ctx, d.log.With().Str("action", "leader-election: campaign").Logger(), func() error {
			err := election.Campaign(ctx, d.Config.Node)
			if err != nil {
				err = session.Close()
				if err != nil {
					d.log.Error().Err(err).Msg("leader-election: failed to close session")
				}
				newSession, err := concurrency.NewSession(d.db.client, concurrency.WithTTL(EtcdLeaseTTL))
				if err != nil {
					d.log.Error().Err(err).Msg("leader-election: failed to create new session")
				} else {
					session = newSession
				}
				election = concurrency.NewElection(session, electionKey)
			}
			return err
		})
		select {
		case <-ctx.Done():
			goto end
		default:
		}
		leaderChan <- LeaderState{Node: d.Config.Node, Key: election.Key()}
		d.log.Info().Str("key", election.Key()).Msg("leader-election: got elected")
		for {
			select {
			case <-ctx.Done():
				goto end
			case <-time.After(LeaderTickerInterval):
				resp, err := election.Leader(ctx)
				if err == concurrency.ErrElectionNoLeader {
					d.log.Info().Msg("leader-election: no leader")
					goto campaign
				} else if err == context.Canceled {
					goto end
				} else if err != nil {
					d.log.Error().Err(err).Msg("leader-election: failed to get leader")
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
	d.log.Debug().Msg("leader-election: context done")
	err := election.Resign(ctx)
	if err != nil {
		d.log.Error().Err(err).Msg("leader-election: failed to resign")
	}
}
