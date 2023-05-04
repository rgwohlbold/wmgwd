package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type LeaderEvent struct {
	IsLeader bool
	Key      string
}

// GenerateLeaderEvents makes the node participate in leader elections. The channel is sent true when the node becomes the leader, and false when it becomes a follower.
func GenerateLeaderEvents(ctx context.Context, db *Database, node string, leaderChan chan<- LeaderEvent) {
	leaderChan <- LeaderEvent{IsLeader: false}
	session, err := concurrency.NewSession(db.client, concurrency.WithTTL(1))
	if err != nil {
		log.Fatal().Err(err).Msg("leader election: failed to create session")
	}
	defer func(session *concurrency.Session) {
		err = session.Close()
		if err != nil {
			log.Error().Err(err).Msg("leader election: failed to close session")
		}
	}(session)
	election := concurrency.NewElection(session, "/wmgwd/leader")
	log.Info().Msg("leader election: campaigning")
	for {
		err = election.Campaign(ctx, node)
		if err != nil {
			log.Fatal().Err(err).Msg("leader election: campaign failed")
		}
		log.Info().Str("key", election.Key()).Msg("leader election: got elected")
		leaderChan <- LeaderEvent{IsLeader: true, Key: election.Key()}
		observeChan := election.Observe(ctx)
		for {
			select {
			case value := <-observeChan:
				if string(value.Kvs[0].Value) == node {
					continue
				}
				log.Info().Msg("leader election: lost election")
				leaderChan <- LeaderEvent{IsLeader: false, Key: election.Key()}
			case <-ctx.Done():
				err = election.Resign(ctx)
				if err != nil {
					log.Fatal().Err(err).Msg("leader election: failed to resign")
				}
				return
			}
		}
	}
}
