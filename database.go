package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const EtcdTimeout = 5 * time.Second
const EtcdMaxTimeout = 600 * time.Second
const EtcdJitter = 5 * time.Millisecond
const EtcdLeaseTTL = 5

const EtcdVniPrefix = "/wmgwd/vni"

const EtcdVniTypeSuffix = "type"
const EtcdVniCurrentSuffix = "current"
const EtcdVniNextSuffix = "next"
const EtcdVniReportSuffix = "report"
const EtcdNodePrefix = "/wmgwd/node/"

const EtcdLeaderPrefix = "/wmgwd/leader/"

type Database struct {
	client *v3.Client
	node   string
	lease  v3.LeaseID
}

type VniLease struct {
	Type  VniLeaseType
	Lease v3.LeaseID
}

var NodeLease = VniLease{Type: NodeLeaseType}
var NoLease = VniLease{Type: NoLeaseType}

type VniLeaseType int

const (
	NodeLeaseType VniLeaseType = iota
	AttachedLeaseType
	NoLeaseType
)

type VniUpdatePart struct {
	Key   string
	Value string
	Lease VniLease
}

type VniUpdate struct {
	vni         uint64
	db          *Database
	parts       []VniUpdatePart
	conditions  []v3.Cmp
	hasRevision bool
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
	client, err := v3.New(v3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	if err != nil {
		return nil, err
	}

	return &Database{client: client, node: config.Node}, nil
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
	_, err := db.client.Revoke(ctx, db.lease)
	if err != nil {
		log.Error().Err(err).Msg("could not revoke lease")
	}
	err = db.client.Close()
	if err != nil {
		log.Error().Err(err).Msg("could not close client")
	}
}

type Node struct {
	Name  string
	Lease v3.LeaseID
}

func (db *Database) Nodes() ([]Node, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	resp, err := db.client.Get(ctx, EtcdNodePrefix, v3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "could not get from etcd")
	}

	nodes := make([]Node, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		nodes[i] = Node{Name: string(kv.Value), Lease: v3.LeaseID(kv.Lease)}
	}

	return nodes, nil
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
	case MigrationEvpnWithdrawn:
		return "migration-evpn-withdrawn"
	case FailoverDecided:
		return "failover-decided"
	default:
		return "unknown"
	}
}

func (db *Database) GetFullState(config Configuration, revision int64) (map[uint64]*VniState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()

	ops := []v3.OpOption{v3.WithPrefix()}
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
		} else if suffix == EtcdVniReportSuffix {
			// ignore
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
		} else if suffix == EtcdVniReportSuffix {
			state.Report, err = strconv.ParseUint(string(kv.Value), 10, 64)
			if err != nil {
				return VniState{}, errors.New("invalid report")
			}
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
	return &VniUpdate{db: db, vni: vni}
}

func (u *VniUpdate) leaseTypeToOption(lease VniLease) []v3.OpOption {
	if lease == NodeLease {
		return []v3.OpOption{v3.WithLease(u.db.lease)}
	} else if lease == NoLease {
		return []v3.OpOption{}
	} else if lease.Type == AttachedLeaseType {
		return []v3.OpOption{v3.WithLease(lease.Lease)}
	}
	log.Fatal().Msg("invalid lease type")
	return nil
}

func (u *VniUpdate) Type(stateType VniStateType) *VniUpdate {
	u.parts = append(u.parts, VniUpdatePart{
		Key:   EtcdVniTypeSuffix,
		Value: strconv.Itoa(int(stateType)),
		Lease: NodeLease,
	})
	return u
}

func (u *VniUpdate) Current(current string, leaseType VniLease) *VniUpdate {
	u.parts = append(u.parts, VniUpdatePart{
		Key:   EtcdVniCurrentSuffix,
		Value: current,
		Lease: leaseType,
	})
	return u
}

func (u *VniUpdate) Next(next string, lease VniLease) *VniUpdate {
	u.parts = append(u.parts, VniUpdatePart{
		Key:   EtcdVniNextSuffix,
		Value: next,
		Lease: lease,
	})
	return u
}

func (u *VniUpdate) Report(report uint64) *VniUpdate {
	u.parts = append(u.parts, VniUpdatePart{
		Key:   EtcdVniReportSuffix,
		Value: strconv.FormatUint(report, 10),
		Lease: NodeLease,
	})
	return u
}

func (u *VniUpdate) Revision(revision int64) *VniUpdate {
	u.conditions = append(u.conditions, v3.Compare(v3.ModRevision(EtcdVniPrefix+"/"+strconv.FormatUint(u.vni, 10)+"/"+EtcdVniTypeSuffix), "<", revision+1))
	u.hasRevision = true
	return u
}

func (u *VniUpdate) LeaderState(leaderState LeaderState) *VniUpdate {
	u.conditions = append(u.conditions, v3.Compare(v3.Value(leaderState.Key), "=", leaderState.Node))
	return u
}

func (u *VniUpdate) Run() {
	if !u.hasRevision {
		panic(errors.New("revision not set"))
	}
	ops := make([]v3.Op, 0)
	l := log.Info()
	for _, part := range u.parts {
		leaseOption := u.leaseTypeToOption(part.Lease)
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
		} else if part.Key == EtcdVniReportSuffix {
			parsedUint, err := strconv.ParseUint(part.Value, 10, 64)
			if err == nil {
				l = l.Uint64("report", parsedUint)
			}
		}
	}
	l.Msg("updating vni")

	go func() {
		timeout := EtcdTimeout
		for timeout <= EtcdMaxTimeout {
			ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
			resp, err := u.db.client.Txn(ctx).If(u.conditions...).Then(ops...).Commit()
			cancel()
			if err != nil {
				log.Warn().Err(err).Msg("failed to update vni, retrying")
			}
			if !resp.Succeeded {
				log.Warn().Msg("transaction to update vni failed, not retrying")
				return
			}
			jitter := time.Duration(rand.Intn(int(2*EtcdJitter)) - int(EtcdJitter))
			timeout = 2*timeout + jitter
		}
	}()

}
