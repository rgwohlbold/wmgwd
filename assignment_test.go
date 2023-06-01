package main

import "testing"

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
	strategies := []AssignmentStrategy{AssignSelf{}, AssignOther{}}
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
