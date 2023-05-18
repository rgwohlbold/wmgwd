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
	return db.setVniState(vni, VniState{
		Type:    FailoverDecided,
		Current: "",
		Next:    d.Config.Node,
	}, VniState{}, leaderState)
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
	return db.setVniState(vni, VniState{
		Type:    FailoverDecided,
		Current: "",
		Next:    node,
	}, VniState{}, leaderState)
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
	for _, vni := range d.Config.Vnis {
		state, err := db.GetState(vni)
		if err != nil {
			return err
		}
		if state.Type == Idle && state.Current == d.Config.Node {
			return db.setVniState(vni, VniState{
				Type:    MigrationDecided,
				Current: d.Config.Node,
				Next:    node,
			}, state, leaderState)
		}
	}
	return nil
}
