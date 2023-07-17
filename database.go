package main

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"strings"
	"time"
)

const EtcdTimeout = 5 * time.Second
const EtcdMaxTimeout = 5 * time.Minute
const EtcdJitter = 50 * time.Millisecond
const EtcdLeaseTTL = 3

const EtcdVniPrefix = "/wmgwd/vni"

const EtcdVniTypeSuffix = "type"
const EtcdVniCurrentSuffix = "current"
const EtcdVniNextSuffix = "next"
const EtcdNodePrefix = "/wmgwd/node/"

type Database struct {
	client *v3.Client
	node   string
	lease  v3.LeaseID
	pool   *VniUpdateWorkerPool
}

type VniStateType int

const (
	Unassigned VniStateType = iota
	Idle
	MigrationDecided
	MigrationConfirmed
	MigrationOspfAdvertised
	MigrationOspfWithdrawn
	MigrationArpEnabled
	MigrationArpDisabled
	MigrationGratuitousArpSent
	MigrationEvpnWithdrawn
	FailoverDecided
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

func NewDatabase(config Configuration) (*Database, error) {
	endpoint := config.EtcdEndpoint
	if endpoint == "" {
		endpoint = "http://localhost:2379"
	}
	client, err := v3.New(v3.Config{
		Endpoints: []string{endpoint},
	})
	if err != nil {
		return nil, err
	}

	return &Database{client: client, node: config.Name, pool: NewVniUpdateWorkerPool(NumVniUpdateWorkers)}, nil
}

func (db *Database) CreateLeaseAndKeepalive(ctx context.Context) (<-chan *v3.LeaseKeepAliveResponse, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(ctx)
	lease, err := createLease(ctx, db.client)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	db.lease = lease.ID

	respChan, err := db.client.KeepAlive(ctx, db.lease)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return respChan, cancel, nil

}

func (db *Database) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	if db.lease != 0 {
		_, err := db.client.Revoke(ctx, db.lease)
		if err != nil {
			log.Error().Err(err).Msg("could not revoke lease")
		}
	}
	err := db.client.Close()
	if err != nil {
		log.Error().Err(err).Msg("could not close client")
	}
}

type Node struct {
	Name  string
	Lease v3.LeaseID
	Uids  []uint64
}

func (db *Database) Nodes(ctx context.Context) ([]Node, error) {
	ctx, cancel := context.WithTimeout(ctx, EtcdTimeout)
	defer cancel()

	resp, err := db.client.Get(ctx, EtcdNodePrefix, v3.WithPrefix(), v3.WithSerializable())
	if err != nil {
		return nil, errors.Wrap(err, "could not get from etcd")
	}

	nodes := make([]Node, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		var uids []uint64
		err = json.Unmarshal(kv.Value, &uids)
		if err != nil {
			return nil, errors.Wrap(err, "could not unmarshal uids")
		}
		name := strings.TrimPrefix(string(kv.Key), EtcdNodePrefix)
		nodes[i] = Node{Name: name, Lease: v3.LeaseID(kv.Lease), Uids: uids}
	}

	return nodes, nil
}

func (db *Database) Register(node string, uids []uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	serialized, err := json.Marshal(uids)
	if err != nil {
		log.Fatal().Err(err).Msg("could not marshal uids")
	}

	_, err = db.client.Put(ctx, EtcdNodePrefix+node, string(serialized), v3.WithLease(db.lease))
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
	case MigrationConfirmed:
		return "migration-confirmed"
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
	default:
		return "unknown"
	}
}

func (db *Database) GetFullState(ctx context.Context, config Configuration, revision int64) (map[uint64]*VniState, error) {
	ctx, cancel := context.WithTimeout(ctx, EtcdTimeout)
	defer cancel()

	ops := []v3.OpOption{v3.WithPrefix(), v3.WithSerializable()}
	if revision != -1 {
		ops = append(ops, v3.WithRev(revision))
	}

	resp, err := db.client.Get(ctx, EtcdVniPrefix+"/", ops...)
	if err != nil {
		return nil, err
	}
	states := make(map[uint64]*VniState)
	for _, vni := range config.Vnis {
		states[vni] = &VniState{Type: Unassigned}
	}
	for _, kv := range resp.Kvs {
		keyVni, err := db.VniFromKv(kv)
		if err != nil {
			return nil, errors.Wrap(err, "could not get keyVni from kv")
		}
		suffix := strings.TrimPrefix(string(kv.Key), EtcdVniPrefix+"/"+strconv.FormatUint(keyVni, 10)+"/")
		if suffix == EtcdVniTypeSuffix {
			value, err := strconv.Atoi(string(kv.Value))
			if err != nil || value < 0 || value >= int(NumStateTypes) {
				return nil, errors.New("invalid state type")
			}
			states[keyVni].Type = VniStateType(value)
		} else if suffix == EtcdVniCurrentSuffix {
			states[keyVni].Current = string(kv.Value)
		} else if suffix == EtcdVniNextSuffix {
			states[keyVni].Next = string(kv.Value)
		} else {
			return nil, errors.New("invalid suffix")
		}
	}
	for _, vni := range config.Vnis {
		states[vni].Revision = resp.Header.Revision
	}
	return states, nil

}

func (db *Database) GetState(vni uint64, revision int64) (VniState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	ops := []v3.OpOption{v3.WithPrefix()}
	if revision != -1 {
		ops = append(ops, v3.WithRev(revision))
	}
	ops = append(ops, v3.WithSerializable())
	resp, err := db.client.Get(ctx, EtcdVniPrefix+"/"+strconv.FormatUint(vni, 10)+"/", ops...)
	if err != nil {
		return VniState{}, err
	}
	state := VniState{Revision: resp.Header.Revision}
	for _, kv := range resp.Kvs {
		suffix := strings.TrimPrefix(string(kv.Key), EtcdVniPrefix+"/"+strconv.FormatUint(vni, 10)+"/")
		if suffix == EtcdVniTypeSuffix {
			value, err := strconv.Atoi(string(kv.Value))
			if err != nil || value < 0 || value >= int(NumStateTypes) {
				return VniState{}, errors.New("invalid state type")
			}
			state.Type = VniStateType(value)
		} else if suffix == EtcdVniCurrentSuffix {
			state.Current = string(kv.Value)
		} else if suffix == EtcdVniNextSuffix {
			state.Next = string(kv.Value)
		} else {
			return VniState{}, errors.New("invalid suffix")
		}
	}
	return state, nil
}

var InvalidKey = errors.New("invalid key")

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

func (db *Database) NewVniUpdate(vni uint64) *VniUpdate {
	return &VniUpdate{db: db, vni: vni, revision: -1}
}
