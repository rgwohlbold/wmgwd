package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	v3 "go.etcd.io/etcd/client/v3"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

const NumVniUpdateWorkers = 1000

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
	vni        uint64
	db         *Database
	parts      []VniUpdatePart
	conditions []v3.Cmp
	revision   int64
	errorChan  chan error
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
		Lease: NoLease,
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

func (u *VniUpdate) Revision(revision int64) *VniUpdate {
	u.conditions = append(u.conditions, v3.Compare(v3.ModRevision(EtcdVniPrefix+"/"+strconv.FormatUint(u.vni, 10)+"/"+EtcdVniTypeSuffix), "<", revision+1))
	u.revision = revision
	return u
}

func (u *VniUpdate) runOnceInternal(ctx context.Context) error {
	if u.revision == -1 {
		panic(errors.New("revision not set"))
	}

	ops := make([]v3.Op, 0)
	hasType := false
	l := log.Debug()
	for _, part := range u.parts {
		if part.Key == EtcdVniTypeSuffix {
			hasType = true
			break
		}
	}
	if hasType {
		l = log.Info()
	}
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
		}
	}
	l = l.Uint64("vni", u.vni).Str("node", u.db.node)
	ctx, cancel := context.WithTimeout(ctx, EtcdTimeout)
	resp, err := u.db.client.Txn(ctx).If(u.conditions...).Then(ops...).Commit()
	cancel()
	if err != nil {
		l.Err(err).Msg("failed to update vni: error")
		return err
	}
	if !resp.Succeeded {
		l.Msg("failed to update vni: transaction failed")
	} else {
		l.Msg("updated vni")
	}
	return nil
}

func (u *VniUpdate) RunOnce() error {
	return u.db.pool.RunOnce(u)
}

func (u *VniUpdate) RunWithRetry() {
	timeout := EtcdTimeout
	for {
		err := u.db.pool.RunOnce(u)
		if err == nil {
			return
		} else if err == context.Canceled {
			return
		} else if err == context.DeadlineExceeded {
			return
		}
		log.Error().Err(err).Msg("failed to update vni, retrying")
		jitter := time.Duration(rand.Intn(int(2*EtcdJitter)) - int(EtcdJitter))
		timeout = 2*timeout + jitter
		if timeout > EtcdMaxTimeout {
			timeout = EtcdMaxTimeout
		}
		time.Sleep(timeout)
	}
}

type VniUpdateWorkerPool struct {
	workers           int
	updatesInProgress map[uint64]int64
	updates           []VniUpdate
	updatesLock       sync.Mutex
	updatesAvailable  sync.Cond
}

func (pool *VniUpdateWorkerPool) VniUpdateWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		var update *VniUpdate
		pool.updatesLock.Lock()
		for update == nil && len(pool.updates) > 0 {
			update = &pool.updates[0]
			if pool.updatesInProgress[update.vni] != update.revision {
				update = nil
			}
			pool.updates = pool.updates[1:]
		}
		pool.updatesLock.Unlock()
		if update == nil {
			// TODO: canceled context?
			pool.updatesAvailable.L.Lock()
			pool.updatesAvailable.Wait()
			pool.updatesAvailable.L.Unlock()
			continue
		}
		err := update.runOnceInternal(ctx)
		pool.updatesLock.Lock()
		if pool.updatesInProgress[update.vni] == update.revision {
			delete(pool.updatesInProgress, update.vni)
		}
		pool.updatesLock.Unlock()
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			update.errorChan <- err
		} else {
			update.errorChan <- nil
		}
	}
}

func NewVniUpdateWorkerPool(workers int) *VniUpdateWorkerPool {
	return &VniUpdateWorkerPool{
		workers:           workers,
		updatesInProgress: make(map[uint64]int64),
		updates:           make([]VniUpdate, 0),
		updatesAvailable:  sync.Cond{L: &sync.Mutex{}},
	}
}

func (pool *VniUpdateWorkerPool) Start(ctx context.Context) {
	for i := 0; i < pool.workers; i++ {
		go pool.VniUpdateWorker(ctx)
	}
}

func (pool *VniUpdateWorkerPool) RunOnce(update *VniUpdate) error {
	pool.updatesLock.Lock()
	if pool.updatesInProgress[update.vni] >= update.revision {
		pool.updatesLock.Unlock()
		return nil
	}
	update.errorChan = make(chan error, 1)
	pool.updatesInProgress[update.vni] = update.revision
	pool.updates = append(pool.updates, *update)
	pool.updatesLock.Unlock()

	pool.updatesAvailable.Signal()
	return <-update.errorChan

}
