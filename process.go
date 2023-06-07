package main

import (
	"context"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type EventProcessor interface {
	Process(ctx context.Context, vniChan chan VniEvent, newNodeChan <-chan NodeEvent) error
}

type DefaultEventProcessor struct {
	daemon *Daemon

	isAssigning *bool
	willAssign  *bool
	assignMutex *sync.Mutex
}

type Verdict int

const (
	VerdictContinue Verdict = iota
	VerdictStop
)

type EventProcessorWrapper struct {
	daemon         *Daemon
	cancel         context.CancelFunc
	eventProcessor EventProcessor
	afterVniEvent  func(*Daemon, VniEvent) Verdict
	beforeVniEvent func(*Daemon, VniEvent) Verdict
}

type AssignmentType int

const (
	Migration AssignmentType = iota
	Failover
)

type Assignment struct {
	Vni   uint64
	State VniState
	Type  AssignmentType
	Next  Node
}

func NoopVniEvent(_ *Daemon, _ VniEvent) Verdict {
	return VerdictContinue
}

func NewDefaultEventProcessor(daemon *Daemon) *DefaultEventProcessor {
	isAssigning := false
	willAssign := false
	return &DefaultEventProcessor{
		daemon:      daemon,
		isAssigning: &isAssigning,
		willAssign:  &willAssign,
		assignMutex: &sync.Mutex{},
	}
}

func (p DefaultEventProcessor) NewVniUpdate(vni uint64) *VniUpdate {
	return p.daemon.db.NewVniUpdate(vni)
}

func (p DefaultEventProcessor) ProcessVniEventSync(event VniEvent) error {
	state := event.State.Type
	current := event.State.Current
	next := event.State.Next
	isCurrent := current == p.daemon.Config.Node
	isNext := next == p.daemon.Config.Node

	// Failure detection
	// All states except Unassigned and Idle need "Next"
	if next == "" && state != Idle && state != Unassigned {
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).RunWithRetry()
	}
	// All states except Unassigned, FailoverDecided need "Current"
	if current == "" && state != Unassigned && state != FailoverDecided {
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Unassigned).Current("", NoLease).Next("", NoLease).RunWithRetry()
	}

	if state == MigrationDecided && isNext {
		err := p.daemon.networkStrategy.AdvertiseEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise evpn")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		err = p.daemon.networkStrategy.AdvertiseOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise ospf")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfAdvertised).RunWithRetry()
	} else if state == MigrationOspfAdvertised && isCurrent {
		err := p.daemon.networkStrategy.WithdrawOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw ospf")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationOspfWithdrawn).RunWithRetry()
	} else if state == MigrationOspfWithdrawn && isNext {
		err := p.daemon.networkStrategy.EnableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not enable arp")
		}
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpEnabled).RunWithRetry()
	} else if state == MigrationArpEnabled && isCurrent {
		err := p.daemon.networkStrategy.DisableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not disable arp")
		}
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationArpDisabled).RunWithRetry()
	} else if state == MigrationArpDisabled && isNext {
		err := p.daemon.networkStrategy.SendGratuitousArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not send gratuitous arp")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationGratuitousArpSent).RunWithRetry()
	} else if state == MigrationGratuitousArpSent && isCurrent {
		err := p.daemon.networkStrategy.WithdrawEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not withdraw evpn")
		}
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(MigrationEvpnWithdrawn).RunWithRetry()
	} else if state == MigrationEvpnWithdrawn && isNext {
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).RunWithRetry()
	} else if state == FailoverDecided && isNext {
		err := p.daemon.networkStrategy.AdvertiseEvpn(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise evpn")
		}
		err = p.daemon.networkStrategy.AdvertiseOspf(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not advertise ospf")
		}
		err = p.daemon.networkStrategy.EnableArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not enable arp")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		err = p.daemon.networkStrategy.SendGratuitousArp(event.Vni)
		if err != nil {
			return errors.Wrap(err, "could not send gratuitous arp")
		}
		time.Sleep(p.daemon.Config.MigrationTimeout)
		p.NewVniUpdate(event.Vni).Revision(event.State.Revision).Type(Idle).Current(event.State.Next, NodeLease).Next("", NoLease).RunWithRetry()
	}
	return nil
}

func (p DefaultEventProcessor) ProcessVniEventAsync(event VniEvent) {
	go func() {
		err := p.ProcessVniEventSync(event)
		if err != nil {
			p.daemon.log.Error().Err(err).Msg("event-processor: failed to process vni event")
		}
	}()
}

func murmur64(key uint64) uint64 {
	key ^= key >> 33
	key *= 0xff51afd7ed558ccd
	key ^= key >> 33
	key *= 0xc4ceb9fe1a85ec53
	key ^= key >> 33
	return key
}

func Assign(nodes []Node, state map[uint64]*VniState) []Assignment {
	if len(nodes) == 0 {
		return nil
	}
	hashNode := treemap.NewWith(utils.UInt64Comparator)
	for _, node := range nodes {
		for _, uid := range node.Uids {
			hashNode.Put(murmur64(uid), node)
		}
	}
	assignments := make([]Assignment, 0)
	for vni, vniState := range state {
		if vniState.Type != Idle && vniState.Type != Unassigned {
			continue
		}
		_, node := hashNode.Ceiling(murmur64(vni))
		if node == nil {
			_, node = hashNode.Min()
		}
		if vniState.Type == Unassigned {
			assignments = append(assignments, Assignment{vni, *vniState, Failover, node.(Node)})
		} else if vniState.Type == Idle && node.(Node).Name != vniState.Current {
			assignments = append(assignments, Assignment{vni, *vniState, Migration, node.(Node)})
		}
	}
	return assignments
}

func (p DefaultEventProcessor) periodicAssignmentInternal(ctx context.Context) error {
	state, err := p.daemon.db.GetFullState(ctx, p.daemon.Config, -1)
	if err != nil {
		p.daemon.log.Error().Err(err).Msg("event-processor: failed to get state on periodic assignment")
		return errors.Wrap(err, "could not get state")
	}
	nodes, err := p.daemon.db.Nodes(ctx)
	if err != nil {
		return errors.Wrap(err, "could not get nodes")
	}
	assignments := Assign(nodes, state)
	for _, assignment := range assignments {
		if assignment.Next.Name == p.daemon.Config.Node {
			if assignment.Type == Migration {
				a := assignment
				go func() {
					err := p.NewVniUpdate(a.Vni).
						Revision(a.State.Revision).
						Type(MigrationDecided).
						Next(a.Next.Name, NodeLease).
						RunOnce()
					if err != nil {
						p.daemon.log.Error().Err(err).Msg("event-processor: failed to assign periodically")
					}
				}()
			} else if assignment.Type == Failover {
				a := assignment
				go func() {
					err := p.NewVniUpdate(a.Vni).
						Revision(a.State.Revision).
						Type(FailoverDecided).
						Next(a.Next.Name, NodeLease).
						RunOnce()
					if err != nil {
						p.daemon.log.Error().Err(err).Msg("event-processor: failed to assign periodically")
					}
				}()
			}
		}
	}
	return nil
}

func (p DefaultEventProcessor) PeriodicAssignmentSync(ctx context.Context) {
	p.assignMutex.Lock()
	if *p.isAssigning {
		*p.willAssign = true
		p.assignMutex.Unlock()
		return
	}
	*p.isAssigning = true
	p.assignMutex.Unlock()
	err := p.periodicAssignmentInternal(ctx)
	if err != nil {
		log.Error().Err(err).Msg("event-processor: failed to run periodic assignment")
	}
	p.assignMutex.Lock()
	if *p.willAssign {
		*p.willAssign = false
		p.assignMutex.Unlock()
		err = p.periodicAssignmentInternal(ctx)
		if err != nil {
			log.Error().Err(err).Msg("event-processor: failed to run periodic assignment")
		}
		p.assignMutex.Lock()
	}
	*p.isAssigning = false
	p.assignMutex.Unlock()
}

func (p DefaultEventProcessor) PeriodicAssignmentAsync(ctx context.Context) {
	go p.PeriodicAssignmentSync(ctx)
}

func reliably(ctx context.Context, log zerolog.Logger, fn func() error) {
	for {
		err := fn()
		if err == nil {
			return
		}
		log.Error().Err(err).Msg("reliably: failed")
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (p DefaultEventProcessor) ProcessAllVnisAsync(ctx context.Context) {
	go func() {
		var states map[uint64]*VniState
		reliably(ctx, p.daemon.log, func() error {
			var err error
			states, err = p.daemon.db.GetFullState(ctx, p.daemon.Config, -1)
			return err
		})
		select {
		case <-ctx.Done():
			return
		default:
		}
		for vni, state := range states {
			p.ProcessVniEventAsync(VniEvent{Vni: vni, State: *state})
		}
	}()
}

func (p DefaultEventProcessor) Process(ctx context.Context, vniChan chan VniEvent, newNodeChan <-chan NodeEvent) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)

	for {
		select {
		case <-ctx.Done():
			p.daemon.log.Debug().Msg("event-processor: context done")
			return nil
		case <-time.After(p.daemon.Config.ScanInterval):
			p.PeriodicAssignmentAsync(ctx)
		case <-newNodeChan:
			p.PeriodicAssignmentAsync(ctx)
		case event := <-vniChan:
			p.ProcessVniEventAsync(event)
		}
	}
}

func (p EventProcessorWrapper) Process(ctx context.Context, vniChan chan VniEvent, newNodeChan <-chan NodeEvent) error {
	innerCtx, innerCancel := context.WithCancel(context.Background())
	internalVniChan := make(chan VniEvent, cap(vniChan))
	internalNewNodeChan := make(chan NodeEvent, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-innerCtx.Done():
				return
			case event := <-vniChan:
				if p.beforeVniEvent(p.daemon, event) == VerdictStop {
					p.cancel()
				}
				internalVniChan <- event
				if p.afterVniEvent(p.daemon, event) == VerdictStop {
					p.cancel()
				}
			case newNodeEvent := <-newNodeChan:
				internalNewNodeChan <- newNodeEvent
			}
		}
	}()
	var err error
	go func() {
		err = p.eventProcessor.Process(ctx, internalVniChan, internalNewNodeChan)
		innerCancel()
		wg.Done()
	}()
	return err
}
