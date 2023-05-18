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

type LeaseType int

const (
	NodeLease LeaseType = iota
	TempLease
	OldLease
	NoLease
)

type VniUpdatePart struct {
	Key       string
	Value     string
	LeaseType LeaseType
}

type VniUpdate struct {
	vni        uint64
	db         *Database
	parts      []VniUpdatePart
	conditions []v3.Cmp
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

func (db *Database) GetState(vni uint64, revision int64) (VniState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	ops := []v3.OpOption{v3.WithPrefix()}
	if revision != -1 {
		ops = append(ops, v3.WithRev(revision))
	}
	resp, err := db.client.Get(ctx, EtcdVniPrefix+"/"+strconv.FormatUint(vni, 10)+"/", ops...)
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

func (db *Database) NewVniUpdate(vni uint64) *VniUpdate {
	return &VniUpdate{db: db, vni: vni}
}

func (u *VniUpdate) leaseTypeToOption(ctx context.Context, leaseType LeaseType) ([]v3.OpOption, error) {
	if leaseType == NodeLease {
		return []v3.OpOption{v3.WithLease(u.db.lease)}, nil
	} else if leaseType == TempLease {
		resp, err := u.db.client.Grant(ctx, int64(EtcdAckTimeout/time.Second))
		if err != nil {
			return nil, errors.Wrap(err, "could not grant lease")
		}
		return []v3.OpOption{v3.WithLease(resp.ID)}, nil
	} else if leaseType == OldLease {
		return []v3.OpOption{v3.WithIgnoreLease()}, nil
	} else if leaseType == NoLease {
		return []v3.OpOption{}, nil
	}
	return nil, errors.New("invalid lease type")
}

func (u *VniUpdate) Type(stateType VniStateType) *VniUpdate {
	u.parts = append(u.parts, VniUpdatePart{
		Key:       EtcdVniTypeSuffix,
		Value:     strconv.Itoa(int(stateType)),
		LeaseType: NodeLease,
	})
	return u
}

func (u *VniUpdate) Current(current string, leaseType LeaseType) *VniUpdate {
	u.parts = append(u.parts, VniUpdatePart{
		Key:       EtcdVniCurrentSuffix,
		Value:     current,
		LeaseType: leaseType,
	})
	return u
}

func (u *VniUpdate) Next(next string, leaseType LeaseType) *VniUpdate {
	u.parts = append(u.parts, VniUpdatePart{
		Key:       EtcdVniNextSuffix,
		Value:     next,
		LeaseType: leaseType,
	})
	return u
}

func (u *VniUpdate) OldCounter(counter int) *VniUpdate {
	u.parts = append(u.parts, VniUpdatePart{
		Key:       EtcdVniCounterSuffix,
		Value:     strconv.Itoa(counter + 1),
		LeaseType: NoLease,
	})
	if counter > 0 {
		u.conditions = append(u.conditions, v3.Compare(v3.Value(EtcdVniPrefix+"/"+strconv.FormatUint(u.vni, 10)+"/"+EtcdVniCounterSuffix), "=", strconv.Itoa(counter)))
	} else {
		u.conditions = append(u.conditions, v3.Compare(v3.CreateRevision(EtcdVniPrefix+"/"+strconv.FormatUint(u.vni, 10)+"/"+EtcdVniCounterSuffix), "=", 0))
	}
	return u
}

func (u *VniUpdate) LeaderState(leaderState LeaderState) *VniUpdate {
	u.conditions = append(u.conditions, v3.Compare(v3.Value(leaderState.Key), "=", leaderState.Node))
	return u
}

func (u *VniUpdate) Run() error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	ops := make([]v3.Op, 0)
	l := log.Info()
	for _, part := range u.parts {
		leaseOption, err := u.leaseTypeToOption(ctx, part.LeaseType)
		if err != nil {
			return err
		}
		ops = append(ops, v3.OpPut(EtcdVniPrefix+"/"+strconv.FormatUint(u.vni, 10)+"/"+part.Key, part.Value, leaseOption...))

		if part.Key == EtcdVniTypeSuffix {
			parsedInt, err := strconv.Atoi(part.Value)
			if err == nil {
				l = l.Str("type", stateTypeToString(VniStateType(parsedInt)))
			}
		} else if part.Key == EtcdVniCurrentSuffix {
			l = l.Str("current", part.Value)
		} else if part.Key == EtcdVniNextSuffix {
			l = l.Str("next", part.Value)
		} else if part.Key == EtcdVniCounterSuffix {
			l = l.Str("counter", part.Value)
		}
	}
	l.Msg("updating vni")
	resp, err := u.db.client.Txn(ctx).If(u.conditions...).Then(ops...).Commit()
	if err != nil {
		return errors.Wrap(err, "failed to update vni")
	}
	if !resp.Succeeded {
		return errors.New("transaction failed")
	}
	return nil

}
