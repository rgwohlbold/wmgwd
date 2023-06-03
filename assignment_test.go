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
		{Name: "node1"},
	}
	state := map[uint64]*VniState{
		1: {Type: Unassigned},
	}
	config := Configuration{Node: "node1"}
	strategies := []AssignmentStrategy{AssignSelf{}, AssignOther{}, AssignGreedy{}}
	for _, strategy := range strategies {
		assignment := strategy.Assign(&Daemon{Config: config}, nodes, state)
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
	assignment := AssignSelf{}.Assign(&Daemon{Config: config}, nodes, state)
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
	assignment := AssignSelf{}.Assign(&Daemon{Config: config}, nodes, state)
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
	assignment := AssignOther{}.Assign(&Daemon{Config: config}, nodes, state)
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
	assignment := AssignOther{}.Assign(&Daemon{Config: config}, nodes, state)
	if len(assignment) != 0 {
		t.Errorf("expected no assignment, got %d", len(assignment))
	}
}

func TestGreedyMigrates(t *testing.T) {
	nodes := []Node{
		{Name: "node1"},
		{Name: "node2"},
	}
	state := map[uint64]*VniState{
		1: {Type: Idle, Current: "node1", Report: 3},
		2: {Type: Idle, Current: "node1", Report: 2},
	}
	config := Configuration{Node: "node1"}
	assignment := AssignGreedy{}.Assign(&Daemon{Config: config}, nodes, state)
	AssertSingleAssignment(t, assignment, Assignment{1, *state[2], Migration, nodes[1]})
}

func TestGreedyMigratesEqually(t *testing.T) {
	nodes := []Node{
		{Name: "node1"},
		{Name: "node2"},
		{Name: "node3"},
	}
	state := map[uint64]*VniState{}
	for i := 1; i <= 300; i++ {
		state[uint64(i)] = &VniState{Type: Idle, Current: "node1", Report: 1}
	}
	config := Configuration{Node: "node1"}
	assignment := AssignGreedy{}.Assign(&Daemon{Config: config}, nodes, state)
	finalAssignment := map[string]int{}
	for _, a := range assignment {
		finalAssignment[a.Next.Name]++
	}
	if finalAssignment["node1"] != 0 {
		t.Errorf("expected 0 assignments to node1, got %d", finalAssignment["node1"])
	}
	if finalAssignment["node2"] != 100 {
		t.Errorf("expected 100 assignments to node2, got %d", finalAssignment["node2"])
	}
	if finalAssignment["node3"] != 100 {
		t.Errorf("expected 100 assignments to node3, got %d", finalAssignment["node3"])
	}
}

func TestGreedyIsNotOptimal(t *testing.T) {
	nodes := []Node{
		{Name: "node1"},
		{Name: "node2"},
	}
	state := map[uint64]*VniState{
		1: {Type: Idle, Current: "node1", Report: 5},
		2: {Type: Idle, Current: "node1", Report: 4},
		3: {Type: Idle, Current: "node1", Report: 3},
		4: {Type: Idle, Current: "node1", Report: 2},
		5: {Type: Idle, Current: "node1", Report: 1},
	}
	config := Configuration{Node: "node1"}
	assignment := AssignGreedy{}.Assign(&Daemon{Config: config}, nodes, state)
	finalUtilization := map[string]uint64{}
	for _, a := range assignment {
		finalUtilization[a.Next.Name] += state[a.Vni].Report
	}
	if finalUtilization["node2"] != 6 && finalUtilization["node2"] != 9 {
		t.Errorf("expected 8 or 10 utilization on node2, got %d", finalUtilization["node1"])
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
	config := Configuration{Node: "node1"}
	assignment := AssignConsistentHashing{}.Assign(&Daemon{Config: config}, nodes, state)
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
	config := Configuration{Node: "node1"}
	assignment := AssignConsistentHashing{}.Assign(&Daemon{Config: config}, nodes, state)
	AssertSingleAssignment(t, assignment, Assignment{1, *state[vni], Failover, nodes[0]})

}
