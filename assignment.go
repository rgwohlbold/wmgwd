package main

import (
	"github.com/pkg/errors"
)

type AssignmentStrategy interface {
	Unassigned(d *Daemon, db *Database, leaderState LeaderState, vni uint64) error
	Periodical(d *Daemon, db *Database, leaderState LeaderState, vni uint64) error
}

type AssignSelf struct{}

func (_ AssignSelf) Unassigned(d *Daemon, db *Database, leaderState LeaderState, vni uint64) error {
	return db.NewVniUpdate(vni).LeaderState(leaderState).Type(FailoverDecided).Current("", NodeLease).Next(d.Config.Node, TempLease).Run()
}

func (_ AssignSelf) Periodical(d *Daemon, db *Database, leaderState LeaderState, vni uint64) error {
	return nil
}

type AssignOther struct{}

func (_ AssignOther) Unassigned(d *Daemon, db *Database, leaderState LeaderState, vni uint64) error {
	nodes, err := db.Nodes()
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return errors.New("no nodes")
	}
	node := nodes[0]
	if node == d.Config.Node && len(nodes) > 1 {
		node = nodes[1]
	}
	return db.NewVniUpdate(vni).LeaderState(leaderState).Type(FailoverDecided).Current("", NodeLease).Next(node, TempLease).Run()
}

func (_ AssignOther) Periodical(d *Daemon, db *Database, leaderState LeaderState, vni uint64) error {
	nodes, err := db.Nodes()
	if err != nil {
		return err
	}
	if len(nodes) < 2 {
		return nil
	}
	node := nodes[0]
	if node == d.Config.Node {
		node = nodes[1]
	}
	state, err := db.GetState(vni)
	if err != nil {
		return err
	}
	if state.Type == Idle && state.Current == d.Config.Node {
		return db.NewVniUpdate(vni).LeaderState(leaderState).Type(FailoverDecided).Current(d.Config.Node, OldLease).Next(node, TempLease).Run()
	}
	return nil
}
