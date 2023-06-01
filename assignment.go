package main

import (
	"github.com/rs/zerolog/log"
)

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

type AssignmentStrategy interface {
	Assign(d *Daemon, nodes []Node, state map[uint64]*VniState) []Assignment
}

type AssignSelf struct{}

func (_ AssignSelf) Assign(d *Daemon, nodes []Node, state map[uint64]*VniState) []Assignment {
	var self Node
	for _, node := range nodes {
		if node.Name == d.Config.Node {
			self = node
		}
	}
	if self.Name == "" {
		log.Error().Msg("could not find self in node list")
		return nil
	}
	assignments := make([]Assignment, 0)
	for vni, vniState := range state {
		if vniState.Type == Unassigned {
			assignments = append(assignments, Assignment{vni, *vniState, Failover, self})
		} else if vniState.Type == Idle && vniState.Current != d.Config.Node {
			assignments = append(assignments, Assignment{vni, *vniState, Migration, self})
		}
	}
	return assignments
}

type AssignOther struct{}

func (_ AssignOther) Assign(d *Daemon, nodes []Node, state map[uint64]*VniState) []Assignment {
	assignments := make([]Assignment, 0)
	node := nodes[0]
	if node.Name == d.Config.Node && len(nodes) > 1 {
		node = nodes[1]
	}
	for vni, vniState := range state {
		if vniState.Type == Idle && vniState.Current == d.Config.Node && node.Name != d.Config.Node {
			assignments = append(assignments, Assignment{vni, *vniState, Migration, node})
		} else if vniState.Type == Unassigned {
			assignments = append(assignments, Assignment{vni, *vniState, Failover, node})
		}
	}
	return assignments
}

type AssignGreedy struct{}

func (_ AssignGreedy) Assign(d *Daemon, nodes []Node, state map[uint64]*VniState) []Assignment {
	return nil
}
