package main

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"time"
)

const EtcdTimeout = 1 * time.Second
const EtcdLeaseTTL = 5

const EtcdVniPrefix = "/wmgwd/vni/"

const EtcdNodePrefix = "/wmgwd/node/"

const EtcdLeaderPrefix = "/wmgwd/leader/"

type Database struct {
	client          *v3.Client
	node            string
	lease           v3.LeaseID
	cancelKeepalive context.CancelFunc
}

type VniStateType int

const (
	Unassigned VniStateType = iota
	Idle
	MigrationDecided
	MigrationOspfAdvertised
	MigrationOspfWithdrawn
	MigrationArpEnabled
	MigrationArpDisabled
	MigrationGratuitousArpSent
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
	return errors.Wrap(err, "could not put to etcd")
}

func stateTypeToString(state VniStateType) string {
	switch state {
	case Unassigned:
		return "unassigned"
	case Idle:
		return "idle"
	case MigrationDecided:
		return "migration-decided"

	case MigrationOspfAdvertised:
		return "migration-ospf-advertised"
	case MigrationOspfWithdrawn:
		return "migration-ospf-withdrawn"
	case MigrationArpEnabled:
		return "migration-arp-enabled"
	case MigrationArpDisabled:
		return "migration-arp-disabled"
	case MigrationGratuitousArpSent:
		return "migration-gratuitous-arp-sent"
	case FailoverDecided:
		return "failover-decided"
	default:
		return "unknown"
	}
}

func (db *Database) setVniState(vni uint64, state VniState, oldState VniState, leaderState LeaderState) error {
	event := log.Debug().Str("type", stateTypeToString(state.Type)).Uint64("vni", vni)
	if state.Current != "" {
		event = event.Str("current", state.Current)
	}
	if state.Next != "" {
		event = event.Str("next", state.Next)
	}
	event.Msg("setting state")

	key := EtcdVniPrefix + strconv.FormatUint(vni, 10)

	serializedState, err := json.Marshal(state)
	if err != nil {
		return errors.Wrap(err, "could not marshal state")
	}

	serializedOldState, err := json.Marshal(oldState)
	if err != nil {
		return errors.Wrap(err, "could not marshal old state")
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
	if err != nil {
		return errors.Wrap(err, "could not put to etcd")
	}
	return nil
}

func (db *Database) GetState(vni uint64) (VniState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	resp, err := db.client.Get(ctx, EtcdVniPrefix+strconv.FormatUint(vni, 10))
	if err != nil {
		return VniState{}, err
	}
	if len(resp.Kvs) == 0 {
		return VniState{Type: Unassigned}, nil
	}
	var state VniState
	err = json.Unmarshal(resp.Kvs[0].Value, &state)
	if err != nil {
		return VniState{}, err
	}
	return state, nil
}
