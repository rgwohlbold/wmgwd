package main

import (
	"github.com/pkg/errors"
)

type AssignmentStrategy interface {
	Unassigned(d *Daemon, state VniState, leaderState LeaderState, vni uint64) error
	Periodical(d *Daemon, state VniState, leaderState LeaderState, vni uint64) error
}

type AssignSelf struct{}

func (_ AssignSelf) Unassigned(d *Daemon, state VniState, leaderState LeaderState, vni uint64) error {
	return d.db.NewVniUpdate(vni).Revision(state.Revision).LeaderState(leaderState).Type(FailoverDecided).Current("", NoLease).Next(d.Config.Node, VniLease{AttachedLeaseType, d.db.lease}).Run()
}

func (_ AssignSelf) Periodical(d *Daemon, state VniState, leaderState LeaderState, vni uint64) error {
	return nil
}

type AssignOther struct{}

func (_ AssignOther) Unassigned(d *Daemon, state VniState, leaderState LeaderState, vni uint64) error {
	nodes, err := d.db.Nodes()
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return errors.New("no nodes")
	}
	node := nodes[0]
	if node.Name == d.Config.Node && len(nodes) > 1 {
		node = nodes[1]
	}
	return d.db.NewVniUpdate(vni).Revision(state.Revision).LeaderState(leaderState).Type(FailoverDecided).Current("", NoLease).Next(node.Name, VniLease{AttachedLeaseType, node.Lease}).Run()
}

func (_ AssignOther) Periodical(d *Daemon, state VniState, leaderState LeaderState, vni uint64) error {
	nodes, err := d.db.Nodes()
	if err != nil {
		return err
	}
	if len(nodes) < 2 {
		return nil
	}
	node := nodes[0]
	if node.Name == d.Config.Node {
		node = nodes[1]
	}
	if state.Type == Idle && state.Current == d.Config.Node {
		return d.db.NewVniUpdate(vni).Revision(state.Revision).LeaderState(leaderState).Type(MigrationDecided).Next(node.Name, VniLease{AttachedLeaseType, node.Lease}).Run()
	}
	return nil
}
