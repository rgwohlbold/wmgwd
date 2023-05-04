package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"time"
)

const EtcdTimeout = 1 * time.Second
const EtcdLeaseTTL = 1
const EtcdLeaseInterval = 200 * time.Millisecond

const EtcdVNIPrefix = "/wmgwd/vni/"

const EtcdNodePrefix = "/wmgwd/node/"

type Database struct {
	*v3.Client
	lease           v3.LeaseID
	cancelKeepalive context.CancelFunc
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

func createLease(ctx context.Context, client *v3.Client) (*v3.LeaseGrantResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, EtcdTimeout)
	defer cancel()

	lease, err := client.Grant(ctx, EtcdLeaseTTL)
	if err != nil {
		return nil, err
	}

	return lease, nil
}

func NewDatabase() (*Database, error) {
	client, err := v3.New(v3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	lease, err := createLease(ctx, client)
	if err != nil {
		cancel()
		return nil, err
	}

	respChan, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		cancel()
		return nil, err
	}
	go func() {
		for range respChan {
			// wait for channel to close
		}
	}()

	return &Database{client, lease.ID, cancel}, nil
}

func (db *Database) Close() error {
	return db.Client.Close()
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
	_, err = db.Client.Put(ctx, EtcdVNIPrefix+strconv.Itoa(vni), string(serialized), v3.WithLease(db.lease))
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
	// TODO: think about the necessity of the database
	_, err = db.Client.Put(ctx, EtcdVNIPrefix+strconv.Itoa(vni)+"/timer", "", v3.WithLease(resp.ID))
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
