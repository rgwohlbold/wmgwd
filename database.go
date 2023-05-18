package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"strings"
	"time"
)

const EtcdTimeout = 500 * time.Second
const EtcdLeaseTTL = 5

const EtcdAckTimeout = 1 * time.Second

const EtcdVniPrefix = "/wmgwd/vni"

const EtcdVniTypeSuffix = "type"
const EtcdVniCurrentSuffix = "current"
const EtcdVniNextSuffix = "next"
const EtcdVniCounterSuffix = "counter"

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
	MigrationAcknowledged
	MigrationOspfAdvertised
	MigrationOspfWithdrawn
	MigrationArpEnabled
	MigrationArpDisabled
	MigrationGratuitousArpSent
	MigrationEvpnWithdrawn
	FailoverDecided
	FailoverAcknowledged
	NumStateTypes
)

var TransactionFailed = errors.New("transaction failed")

func createLease(ctx context.Context, client *v3.Client) (*v3.LeaseGrantResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, EtcdTimeout)
	defer cancel()

	lease, err := client.Grant(ctx, EtcdLeaseTTL)
	if err != nil {
		return nil, err
	}

	return lease, nil
}

func NewDatabase(ctx context.Context, config Configuration) (*Database, error) {
	client, err := v3.New(v3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)

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

	return &Database{client, config.Node, lease.ID, cancel}, nil
}

func (db *Database) Close() {
	db.cancelKeepalive()
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	_, err := db.client.Revoke(ctx, db.lease)
	if err != nil {
		log.Error().Err(err).Msg("could not revoke lease")
	}
	err = db.client.Close()
	if err != nil {
		log.Error().Err(err).Msg("could not close client")
	}
}

func (db *Database) Nodes() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	resp, err := db.client.Get(ctx, EtcdNodePrefix, v3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "could not get from etcd")
	}

	nodes := make([]string, 0)
	for _, kv := range resp.Kvs {
		nodes = append(nodes, strings.TrimPrefix(string(kv.Key), EtcdNodePrefix))
	}

	return nodes, nil
}

func (db *Database) Register(node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	_, err := db.client.Put(ctx, EtcdNodePrefix+node, node, v3.WithLease(db.lease))
	return errors.Wrap(err, "could not put to etcd")
}

func (db *Database) Unregister(node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	_, err := db.client.Delete(ctx, EtcdNodePrefix+node)
	return errors.Wrap(err, "could not delete from etcd")
}

func stateTypeToString(state VniStateType) string {
	switch state {
	case Unassigned:
		return "unassigned"
	case Idle:
		return "idle"
	case MigrationDecided:
		return "migration-decided"
	case MigrationAcknowledged:
		return "migration-acknowledged"
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
	case MigrationEvpnWithdrawn:
		return "migration-evpn-withdrawn"
	case FailoverDecided:
		return "failover-decided"
	case FailoverAcknowledged:
		return "failover-acknowledged"
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

	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	keyPrefix := EtcdVniPrefix + "/" + strconv.FormatUint(vni, 10)

	var conditions []v3.Cmp
	var ops []v3.Op
	if oldState.Counter != 0 {
		conditions = append(conditions, v3.Compare(v3.Value(keyPrefix+"/"+EtcdVniCounterSuffix), "=", strconv.Itoa(oldState.Counter)))
	}
	if leaderState.Node == db.node {
		conditions = append(conditions, v3.Compare(v3.Value(leaderState.Key), "=", leaderState.Node))
	}

	ops = append(ops, v3.OpPut(keyPrefix+"/"+EtcdVniTypeSuffix, strconv.Itoa(int(state.Type)), v3.WithLease(db.lease)))
	if oldState.Current == state.Current {
		if state.Type == FailoverAcknowledged || state.Type == MigrationAcknowledged {
			ops = append(ops, v3.OpPut(keyPrefix+"/"+EtcdVniCurrentSuffix, state.Current, v3.WithLease(db.lease)))
		} else if state.Current != "" {
			// If Next is set and the value does not change, keep the lease as-is to add it to the revision history
			ops = append(ops, v3.OpPut(keyPrefix+"/"+EtcdVniCurrentSuffix, state.Current, v3.WithIgnoreLease()))
		}
	} else {
		ops = append(ops, v3.OpPut(keyPrefix+"/"+EtcdVniCurrentSuffix, state.Current, v3.WithLease(db.lease)))
	}
	if oldState.Next == state.Next {
		if state.Next != "" {
			// If Next is set and the value does not change, keep the lease as-is to add it to the revision history
			ops = append(ops, v3.OpPut(keyPrefix+"/"+EtcdVniNextSuffix, state.Next, v3.WithIgnoreLease()))
		}
	} else if state.Type == FailoverDecided || state.Type == MigrationDecided {
		// Nodes are assigned to these states by the leader, so we need to grant a separate lease to start a timeout
		resp, err := db.client.Grant(ctx, int64(EtcdAckTimeout/time.Second))
		if err != nil {
			return errors.Wrap(err, "could not grant lease")
		}
		ops = append(ops, v3.OpPut(keyPrefix+"/"+EtcdVniNextSuffix, state.Next, v3.WithLease(resp.ID)))
	} else {
		ops = append(ops, v3.OpPut(keyPrefix+"/"+EtcdVniNextSuffix, state.Next, v3.WithLease(db.lease)))
	}
	ops = append(ops, v3.OpPut(keyPrefix+"/"+EtcdVniCounterSuffix, strconv.Itoa(oldState.Counter+1)))

	resp, err := db.client.Txn(ctx).If(conditions...).Then(ops...).Commit()
	if err != nil {
		return errors.Wrap(err, "could not put to etcd")
	}
	if !resp.Succeeded {
		return TransactionFailed
	}
	return nil
}

func (db *Database) GetState(vni uint64) (VniState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	resp, err := db.client.Get(ctx, EtcdVniPrefix+"/"+strconv.FormatUint(vni, 10)+"/", v3.WithPrefix())
	if err != nil {
		return VniState{}, err
	}
	event, err := db.VniEventFromKvs(resp.Kvs, vni)
	if err != nil {
		return VniState{}, err
	}
	return event.State, nil
}

const InvalidVni = ^uint64(0)

var InvalidKey = errors.New("invalid key")
var InvalidValue = errors.New("invalid value")

func (db *Database) VniFromKv(kv *mvccpb.KeyValue) (uint64, error) {
	keyRest := strings.TrimPrefix(string(kv.Key), EtcdVniPrefix+"/")
	parts := strings.Split(keyRest, "/")
	if len(parts) != 2 {
		return 0, InvalidKey
	}
	parsedVni, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		log.Error().Str("key", string(kv.Key)).Str("vni", keyRest).Err(err).Msg("vni-watcher: failed to parse vni")
		return 0, InvalidKey
	}
	return parsedVni, nil
}

func (db *Database) VniEventFromKvs(kvs []*mvccpb.KeyValue, inputVni uint64) (VniEvent, error) {
	vni := inputVni
	state := VniState{}
	for _, kv := range kvs {
		parsedVni, err := db.VniFromKv(kv)
		if err != nil {
			return VniEvent{}, err
		}
		if vni != InvalidVni && parsedVni != vni {
			return VniEvent{}, errors.New("vni mismatch between kvs")
		}
		vni = parsedVni

		suffix := strings.TrimPrefix(string(kv.Key), EtcdVniPrefix+"/"+strconv.FormatUint(vni, 10)+"/")
		if suffix == EtcdVniTypeSuffix {
			value, err := strconv.Atoi(string(kv.Value))
			if err != nil || value < 0 || value >= int(NumStateTypes) {
				return VniEvent{}, errors.New("invalid state type")
			}
			state.Type = VniStateType(value)
		} else if suffix == EtcdVniCurrentSuffix {
			state.Current = string(kv.Value)
		} else if suffix == EtcdVniNextSuffix {
			state.Next = string(kv.Value)
		} else if suffix == EtcdVniCounterSuffix {
			value, err := strconv.Atoi(string(kv.Value))
			if err != nil || value < 0 {
				return VniEvent{}, errors.New("invalid counter")
			}
			state.Counter = value
		} else {
			return VniEvent{}, errors.New("invalid suffix")
		}
	}
	if vni == InvalidVni {
		return VniEvent{}, errors.New("no vni found")
	}
	return VniEvent{Vni: vni, State: state}, nil
}
