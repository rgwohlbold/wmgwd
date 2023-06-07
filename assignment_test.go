package main

import (
	"math"
	"testing"
)

func AssertSingleAssignment(t *testing.T, assignments []Assignment, expected Assignment) {
	if len(assignments) != 1 {
		t.Errorf("expected 1 assignment, got %d", len(assignments))
	}
	if assignments[0].Type != expected.Type {
		t.Errorf("expected %d assignment, got %d", expected.Type, assignments[0].Type)
	}
	if assignments[0].Next.Name != expected.Next.Name {
		t.Errorf("expected assignment to %s, got %s", expected.Next.Name, assignments[0].Next.Name)
	}
}

func TestAssignsUnassigned(t *testing.T) {
	nodes := []Node{
		{Name: "node1", Uids: []uint64{0}},
	}
	state := map[uint64]*VniState{
		1: {Type: Unassigned},
	}
	config := Configuration{Node: "node1"}
	strategies := []AssignmentStrategy{AssignSelf{Config: &config}, AssignOther{Config: &config}, AssignConsistentHashing{}}
	for _, strategy := range strategies {
		assignment := strategy.Assign(nodes, state)
		AssertSingleAssignment(t, assignment, Assignment{1, *state[1], Failover, nodes[0]})
	}
}

func TestAssignSelfMigrates(t *testing.T) {
	nodes := []Node{
		{Name: "node1"},
		{Name: "node2"},
	}
	state := map[uint64]*VniState{
		1: {Type: Idle, Current: "node1"},
	}
	config := Configuration{Node: "node2"}
	assignment := AssignSelf{Config: &config}.Assign(nodes, state)
	AssertSingleAssignment(t, assignment, Assignment{1, *state[1], Migration, nodes[1]})
}

func TestAssignSelfDoesNothing(t *testing.T) {
	nodes := []Node{
		{Name: "node1"},
		{Name: "node2"},
	}
	state := map[uint64]*VniState{
		1: {Type: Idle, Current: "node1"},
	}
	config := Configuration{Node: "node1"}
	assignment := AssignSelf{Config: &config}.Assign(nodes, state)
	if len(assignment) != 0 {
		t.Errorf("expected no assignment, got %d", len(assignment))
	}
}

func TestAssignOtherMigrates(t *testing.T) {
	nodes := []Node{
		{Name: "node1"},
		{Name: "node2"},
	}
	state := map[uint64]*VniState{
		1: {Type: Idle, Current: "node1"},
	}
	config := Configuration{Node: "node1"}
	assignment := AssignOther{Config: &config}.Assign(nodes, state)
	AssertSingleAssignment(t, assignment, Assignment{1, *state[1], Migration, nodes[1]})

}
func TestAssignOtherDoesNothing(t *testing.T) {
	nodes := []Node{
		{Name: "node1"},
		{Name: "node2"},
	}
	state := map[uint64]*VniState{
		1: {Type: Idle, Current: "node2"},
	}
	config := Configuration{Node: "node1"}
	assignment := AssignOther{Config: &config}.Assign(nodes, state)
	if len(assignment) != 0 {
		t.Errorf("expected no assignment, got %d", len(assignment))
	}
}

func FindVniThatMapsBetween(lower, higher uint64) uint64 {
	for i := uint64(0); i < math.MaxUint64; i++ {
		if murmur64(i) >= lower && murmur64(i) < higher {
			return i
		}
	}
	panic("could not find uid")
}

func TestConsistentHashingAssignsToNextHigher(t *testing.T) {
	lower := uint64(math.MaxUint64 / 20)
	higher := uint64(math.MaxUint64 / 10)
	nodes := []Node{
		{Name: "node1", Uids: []uint64{lower}},
		{Name: "node2", Uids: []uint64{higher}},
	}
	vni := FindVniThatMapsBetween(lower, higher)
	state := map[uint64]*VniState{
		vni: {Type: Unassigned},
	}
	assignment := AssignConsistentHashing{}.Assign(nodes, state)
	AssertSingleAssignment(t, assignment, Assignment{1, *state[vni], Failover, nodes[1]})
}

func TestConsistentHashingWrapsAround(t *testing.T) {
	lower := uint64(math.MaxUint64 / 20)
	higher := uint64(math.MaxUint64 / 10)
	nodes := []Node{
		{Name: "node1", Uids: []uint64{lower}},
		{Name: "node2", Uids: []uint64{higher}},
	}
	vni := FindVniThatMapsBetween(higher+1, math.MaxUint64)
	state := map[uint64]*VniState{
		vni: {Type: Unassigned},
	}
	assignment := AssignConsistentHashing{}.Assign(nodes, state)
	AssertSingleAssignment(t, assignment, Assignment{1, *state[vni], Failover, nodes[0]})

}
