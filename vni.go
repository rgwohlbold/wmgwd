package main

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"strings"
)

func GenerateLeaderChangeEvents(ch chan<- VNIEvent, db *Database, vnis []int) {
	for _, vni := range vnis {
		state, err := db.GetState(vni)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to get state")
		}
		ch <- VNIEvent{VNI: vni, State: state}
	}
}

func GenerateWatchEvents(node string, ch chan<- VNIEvent) {
	db, err := NewDatabase(node)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer db.Close()

	watchChan := db.client.Watch(context.Background(), EtcdVNIPrefix, v3.WithPrefix())
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

			if ev.Type == v3.EventTypeDelete {
				state.Type = Unassigned
			} else {
				err = json.Unmarshal(ev.Kv.Value, &state)
				if err != nil {
					log.Error().Str("key", string(ev.Kv.Key)).Str("value", string(ev.Kv.Value)).Err(err).Msg("failed to parse state")
					continue
				}
			}

			ch <- VNIEvent{VNI: vni, State: state}
		}
	}

}

func ProcessVNIEvent(node string, leader LeaderEvent, event VNIEvent, frr *FRRClient, db *Database) error {
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
			AddMigrationTimer(event.VNI)
			return db.SetMigrationTimerStarted(event.VNI, event.State.Current, event.State.Next)
		}
	} else if event.State.Type == FailoverDecided {
		if node == event.State.Next {
			err := frr.Advertise(event.VNI)
			if err != nil {
				return err
			}
			// problem: since the old node sets idle, it will use its lease. when it fails, the key is not deleted
			return db.SetIdle(event.VNI, event.State.Next)
		}
	}
	return nil
}
