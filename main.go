package main

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"os"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	State VNIState
	VNI   int
}

const MigrationTimeout = 30 * time.Second

func ProcessEvent(node string, leader LeaderElectionResult, event Event, frr *FRRClient, db *Database) error {
	if event.State.Type == Unassigned {
		if leader.IsLeader {
			return db.SetFailoverDecided(event.VNI, event.State.Current, node)
		}
	} else if event.State.Type == MigrationDecided {
		if node == event.State.Next {
			err := frr.Advertise(event.VNI)
			if err != nil {
				return err
			}
			return db.SetMigrationInterfacesCreated(event.VNI, event.State.Current, event.State.Next)
		}
	} else if event.State.Type == MigrationInterfacesCreated {
		if node == event.State.Current {
			err := frr.Withdraw(event.VNI)
			if err != nil {
				return err
			}
			err = db.StartMigrationTimer(event.VNI)
			if err != nil {
				return err
			}
			return db.SetMigrationTimerStarted(event.VNI, event.State.Current, event.State.Next)
		}
	} else if event.State.Type == MigrationTimerExpired {
		if node == event.State.Next {
			return db.SetIdle(event.VNI, event.State.Next)
		}
	} else if event.State.Type == FailoverDecided {
		if node == event.State.Next {
			err := frr.Advertise(event.VNI)
			if err != nil {
				return err
			}
			return db.SetIdle(event.VNI, event.State.Next)
		}
	} else if event.State.Type == Idle {
		/* On restart, we have previously withdrawn all routes.
		   We now may need to re-advertise the VNIs that are assigned to us. */
		if node == event.State.Current {
			return frr.Advertise(event.VNI)
		}
	}
	return nil
}

func ProcessEvents(node string, eventChan chan Event, leaderChan <-chan LeaderElectionResult, frr *FRRClient, db *Database, vnis []int) {
	leader := <-leaderChan
	for {
		select {
		case event := <-eventChan:
			err := ProcessEvent(node, leader, event, frr, db)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to process event")
			}
		case leader = <-leaderChan:
			GenerateLeaderChangeEvents(eventChan, db, vnis)
		}
	}
}

func GenerateLeaderChangeEvents(ch chan<- Event, db *Database, vnis []int) {
	for _, vni := range vnis {
		state, err := db.GetState(vni)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to get state")
		}
		if state.Type == Unassigned {
			ch <- Event{VNI: vni, State: VNIState{Type: Unassigned, Current: state.Current}}
		} else if state.Type == MigrationDecided {
			ch <- Event{VNI: vni, State: VNIState{Type: MigrationDecided, Current: state.Current, Next: state.Next}}
		} else if state.Type == MigrationInterfacesCreated {
			ch <- Event{VNI: vni, State: VNIState{Type: MigrationInterfacesCreated, Current: state.Current, Next: state.Next}}
		} else if state.Type == MigrationTimerStarted {
			ch <- Event{VNI: vni, State: VNIState{Type: MigrationTimerStarted, Current: state.Current, Next: state.Next}}
		} else if state.Type == FailoverDecided {
			ch <- Event{VNI: vni, State: VNIState{Type: FailoverDecided, Current: state.Current, Next: state.Next}}
		} else if state.Type == Idle {
			ch <- Event{VNI: vni, State: VNIState{Type: Idle, Current: state.Current}}
		}
	}

}

func GenerateWatchEvents(ch chan<- Event) {
	db, err := NewDatabase()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer db.Close()

	watchChan := db.Client.Watch(context.Background(), EtcdVNIPrefix, clientv3.WithPrefix())
	for {
		e := <-watchChan
		for _, ev := range e.Events {
			keyRest := strings.TrimPrefix(string(ev.Kv.Key), EtcdVNIPrefix)
			vni, err := strconv.Atoi(keyRest)
			if err != nil {
				log.Error().Str("key", string(ev.Kv.Key)).Str("vni", keyRest).Err(err).Msg("failed to parse vni")
				continue
			}
			state := VNIState{}
			err = json.Unmarshal(ev.Kv.Value, &state)
			if err != nil {
				log.Error().Str("key", string(ev.Kv.Key)).Str("value", string(ev.Kv.Value)).Err(err).Msg("failed to parse state")
				continue
			}

			ch <- Event{VNI: vni, State: state}
		}
	}

}

type LeaderElectionResult struct {
	IsLeader bool
	Key      string
}

// LeaderElectionLoop makes the node participate in leader elections. The channel is sent true when the node becomes the leader, and false when it becomes a follower.
func LeaderElectionLoop(ctx context.Context, db *Database, node string, leaderChan chan<- LeaderElectionResult) {
	leaderChan <- LeaderElectionResult{IsLeader: false}
	session, err := concurrency.NewSession(db.Client, concurrency.WithTTL(1))
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
		leaderChan <- LeaderElectionResult{IsLeader: true, Key: election.Key()}
		observeChan := election.Observe(ctx)
		for {
			select {
			case value := <-observeChan:
				if string(value.Kvs[0].Value) == node {
					continue
				}
				log.Info().Msg("leader election: lost election")
				leaderChan <- LeaderElectionResult{IsLeader: false, Key: election.Key()}
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

func main() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	if len(os.Args) != 2 {
		log.Fatal().Msg("usage: wmgwd <node>")
	}
	node := os.Args[1]

	frr := NewFRRClient()

	vnis := []int{100, 200, 300}
	for _, vni := range vnis {
		err := frr.Withdraw(vni)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to withdraw")
		}
	}

	ctx := context.Background()

	db, err := NewDatabase()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer db.Close()

	log.Info().Msg("starting leader election loop")
	leaderChan := make(chan LeaderElectionResult)
	eventChan := make(chan Event, len(vnis))

	go LeaderElectionLoop(ctx, db, node, leaderChan)
	go GenerateWatchEvents(eventChan)
	go ProcessEvents(node, eventChan, leaderChan, frr, db, vnis)

	<-ctx.Done()
}
