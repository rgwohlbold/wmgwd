package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rs/zerolog/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"time"
)

const EtcdTimeout = 1 * time.Second
const EtcdLeaseTTL = 1
const EtcdLeaseInterval = 200 * time.Millisecond

const EtcdVNIPrefix = "/wmgwd/vni/"

type Database struct {
	*clientv3.Client
}

type VNIStateType int

const (
	Unassigned VNIStateType = iota
	Idle
	MigrationDecided
	MigrationInterfacesCreated
	MigrationTimerStarted
	FailoverDecided
	MigrationTimerExpired
)

type VNIState struct {
	Type    VNIStateType `json:"type"`
	Current string       `json:"current"`
	Next    string       `json:"next"`
}

func NewDatabase() (*Database, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	if err != nil {
		return nil, err
	}

	return &Database{client}, nil
}

func (db *Database) Close() error {
	return db.Client.Close()
}

func (db *Database) Events() {
	c := db.Client.Watch(context.Background(), "", clientv3.WithPrefix())
	for {
		w := <-c
		events := w.Events
		for _, e := range events {
			log.Info().Str("key", string(e.Kv.Key)).Str("value", string(e.Kv.Value)).Msg("event")
		}
	}
}

func (db *Database) ExtendLeaseOnce(ctx context.Context, node string) error {
	lease, err := db.Client.Grant(ctx, EtcdLeaseTTL)
	if err != nil {
		return err
	}
	_, err = db.Client.Put(ctx, "/wmgwd/nodes/"+node, "", clientv3.WithLease(lease.ID))
	if err != nil {
		return err
	}
	log.Debug().Msg("extended lease")
	return err
}

func (db *Database) ExtendLeaseForever(node string) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
		err := db.ExtendLeaseOnce(ctx, node)
		cancel()
		if err != nil {
			log.Error().Err(err).Msg("failed to extend lease")
		}
		time.Sleep(EtcdLeaseInterval)
	}
}

func (db *Database) Leader() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	resp, err := db.Client.MemberList(ctx)
	defer cancel()

	if err != nil {
		return "", err
	}
	for _, m := range resp.Members {
		if !m.GetIsLearner() {
			return m.Name, nil
		}
	}
	return "", errors.New("no leader found")
}

func (db *Database) setVNIState(vni int, state VNIState) error {
	log.Debug().Str("type", string(state.Type)).Int("vni", vni).Msg("setting state")

	serialized, err := json.Marshal(state)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	_, err = db.Client.Put(ctx, EtcdVNIPrefix+strconv.Itoa(vni), string(serialized))
	defer cancel()
	return err
}

func (db *Database) StartMigrationTimer(vni int) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	resp, err := db.Client.Grant(ctx, int64(MigrationTimeout/time.Second))
	if err != nil {
		return err
	}
	_, err = db.Client.Put(ctx, EtcdVNIPrefix+strconv.Itoa(vni)+"/timer", "", clientv3.WithLease(resp.ID))
	return err
}

func (db *Database) SetFailoverDecided(vni int, current string, next string) error {
	return db.setVNIState(vni, VNIState{
		Type:    FailoverDecided,
		Current: current,
		Next:    next,
	})
}

func (db *Database) SetMigrationInterfacesCreated(vni int, current string, next string) error {
	return db.setVNIState(vni, VNIState{
		Type:    MigrationInterfacesCreated,
		Current: current,
		Next:    next,
	})

}

func (db *Database) SetMigrationTimerStarted(vni int, current string, next string) error {
	return db.setVNIState(vni, VNIState{
		Type:    MigrationTimerStarted,
		Current: current,
		Next:    next,
	})
}

func (db *Database) SetIdle(vni int, next string) error {
	return db.setVNIState(vni, VNIState{
		Type:    Idle,
		Current: next,
		Next:    "",
	})
}

func (db *Database) GetState(vni int) (VNIState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	resp, err := db.Client.Get(ctx, EtcdVNIPrefix+strconv.Itoa(vni))
	if err != nil {
		return VNIState{}, err
	}
	if len(resp.Kvs) == 0 {
		return VNIState{Type: Unassigned}, nil
	}
	var state VNIState
	err = json.Unmarshal(resp.Kvs[0].Value, &state)
	if err != nil {
		return VNIState{}, err
	}
	return state, nil
}
