package main

import (
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
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

type HierarchyMap[K1 comparable, K2 comparable, V any] struct {
	m  *treemap.Map
	c2 utils.Comparator
}

func NewMultiMap[K1 comparable, K2 comparable, V any](c1 utils.Comparator, c2 utils.Comparator) HierarchyMap[K1, K2, V] {
	return HierarchyMap[K1, K2, V]{treemap.NewWith(c1), c2}
}

func (m HierarchyMap[K1, K2, V]) Add(key1 K1, key2 K2, value V) {
	m2, ok := m.m.Get(key1)
	if !ok {
		m.m.Put(key1, treemap.NewWith(m.c2))
		m2, _ = m.m.Get(key1)
	}
	m2.(*treemap.Map).Put(key2, value)
}

func (m HierarchyMap[K1, K2, V]) Get(key1 K1, key2 K2) (V, bool) {
	value, ok := m.m.Get(key1)
	if !ok {
		var r2 V
		return r2, false
	}
	m2 := value.(*treemap.Map)
	value, ok = m2.Get(key2)
	if !ok {
		var r2 V
		return r2, false
	}
	return value.(V), true
}

func (m HierarchyMap[K1, K2, V]) GetAny(key1 K1) (K2, V, bool) {
	value, ok := m.m.Get(key1)
	if !ok {
		var r2 K2
		var r3 V
		return r2, r3, false
	}
	m2 := value.(*treemap.Map)
	key, value := m2.Max()
	if key == nil {
		var r2 K2
		var r3 V
		return r2, r3, false
	}
	return key.(K2), value.(V), true
}

func (m HierarchyMap[K1, K2, V]) Remove(key1 K1, key2 K2) {
	value, ok := m.m.Get(key1)
	if !ok {
		return
	}
	m2 := value.(*treemap.Map)
	m2.Remove(key2)
	if m2.Empty() {
		m.m.Remove(key1)
	}
}

func (m HierarchyMap[K1, K2, V]) Empty() bool {
	return m.m.Empty()
}

func (m HierarchyMap[K1, K2, V]) Max() K1 {
	key, _ := m.m.Max()
	return key.(K1)
}

func (m HierarchyMap[K1, K2, V]) Min() K1 {
	key, _ := m.m.Min()
	return key.(K1)
}

type AssignGreedy struct{}

func (_ AssignGreedy) Assign(d *Daemon, nodes []Node, state map[uint64]*VniState) []Assignment {
	// determine utilization of each node and total utilization
	utilization := make(map[string]uint64)
	totalUtilization := uint64(0)
	for _, node := range nodes {
		utilization[node.Name] = 0
	}
	for _, vniState := range state {
		if vniState.Type == Idle || vniState.Type == Unassigned {
			utilization[vniState.Current] += vniState.Report
			totalUtilization += vniState.Report
		}
	}

	// sort nodes by total utilization
	nodeMap := NewMultiMap[uint64, string, Node](utils.UInt64Comparator, utils.StringComparator)
	for _, node := range nodes {
		nodeMap.Add(utilization[node.Name], node.Name, node)
	}

	utilizationThreshold := totalUtilization / uint64(len(nodes))

	// determine vnis to migrate/failover
	vniMap := NewMultiMap[uint64, uint64, VniEvent](utils.UInt64Comparator, utils.UInt64Comparator)
	for vni, vniState := range state {
		if (vniState.Type == Idle && utilization[vniState.Current] > utilizationThreshold) || vniState.Type == Unassigned {
			vniMap.Add(vniState.Report, vni, VniEvent{*vniState, vni})
		}
	}

	// for each vni, assign to most under-utilized node
	assignments := make([]Assignment, 0)
	for !vniMap.Empty() {
		vniUtilization := vniMap.Max()
		vni, event, _ := vniMap.GetAny(vniUtilization)
		vniMap.Remove(vniUtilization, vni)

		if event.State.Type == Unassigned || event.State.Type == Idle {
			currentNodeUtilization := utilization[event.State.Current]
			currentNode, _ := nodeMap.Get(currentNodeUtilization, event.State.Current)
			nodeMap.Remove(currentNodeUtilization, event.State.Current)
			nodeMap.Add(currentNodeUtilization-vniUtilization, event.State.Current, currentNode)
			utilization[event.State.Current] -= vniUtilization

			minUtilization := nodeMap.Min()
			_, nextNode, _ := nodeMap.GetAny(minUtilization)
			nodeMap.Remove(minUtilization, nextNode.Name)
			nodeMap.Add(minUtilization+vniUtilization, nextNode.Name, nextNode)
			utilization[nextNode.Name] = minUtilization + vniUtilization

			if nextNode.Name != event.State.Current {
				assignments = append(assignments, Assignment{event.Vni, event.State, Failover, nextNode})
			}
		}
	}
	return assignments
}
