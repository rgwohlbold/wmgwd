package main

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"time"
)

const EtcdTimeout = 1 * time.Second
const EtcdLeaseTTL = 5

const EtcdVNIPrefix = "/wmgwd/vni/"

const EtcdNodePrefix = "/wmgwd/node/"

const EtcdLeaderPrefix = "/wmgwd/leader/"

type Database struct {
	client          *v3.Client
	node            string
	lease           v3.LeaseID
	cancelKeepalive context.CancelFunc
}

type VNIStateType int

const (
	Unassigned VNIStateType = iota
	Idle
	MigrationDecided
	MigrationInterfacesCreated
	MigrationCostReduced
	FailoverDecided
)

func createLease(ctx context.Context, client *v3.Client) (*v3.LeaseGrantResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, EtcdTimeout)
	defer cancel()

	lease, err := client.Grant(ctx, EtcdLeaseTTL)
	if err != nil {
		return nil, err
	}

	return lease, nil
}

func NewDatabase(node string) (*Database, error) {
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

	return &Database{client, node, lease.ID, cancel}, nil
}

func (db *Database) Close() error {
	return db.client.Close()
}

func (db *Database) Register(node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	_, err := db.client.Put(ctx, EtcdNodePrefix+node, node, v3.WithLease(db.lease))
	return err
}

func stateTypeToString(state VNIStateType) string {
	switch state {
	case Unassigned:
		return "unassigned"
	case Idle:
		return "idle"
	case MigrationDecided:
		return "migration-decided"
	case MigrationInterfacesCreated:
		return "migration-interfaces-created"
	case MigrationCostReduced:
		return "migration-cost-reduced"
	case FailoverDecided:
		return "failover-decided"
	default:
		return "unknown"
	}
}

func (db *Database) setVNIState(vni int, state VNIState, oldState VNIState, leaderState LeaderState) error {
	if state.Next == "" {
		log.Debug().Str("type", stateTypeToString(state.Type)).Int("vni", vni).Str("current", state.Current).Msg("setting state")
	} else {
		log.Debug().Str("type", stateTypeToString(state.Type)).Int("vni", vni).Str("current", state.Current).Str("next", state.Next).Msg("setting state")
	}

	key := EtcdVNIPrefix + strconv.Itoa(vni)

	serializedState, err := json.Marshal(state)
	if err != nil {
		return err
	}

	serializedOldState, err := json.Marshal(oldState)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	var conditions []v3.Cmp
	if oldState.Type == Unassigned {
		conditions = append(conditions, v3.Compare(v3.CreateRevision(key), "=", 0))
	} else {
		conditions = append(conditions, v3.Compare(v3.Value(key), "=", string(serializedOldState)))
	}
	if leaderState.Node == db.node {
		conditions = append(conditions, v3.Compare(v3.Value(leaderState.Key), "=", leaderState.Node))
	}
	_, err = db.client.Txn(ctx).If(conditions...).Then(v3.OpPut(key, string(serializedState), v3.WithLease(db.lease))).Commit()
	return err
}

func (db *Database) GetState(vni int) (VNIState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	resp, err := db.client.Get(ctx, EtcdVNIPrefix+strconv.Itoa(vni))
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
