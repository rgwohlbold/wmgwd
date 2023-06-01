package main

type AssignmentStrategy interface {
	Assign(d *Daemon, leaderState LeaderState, nodes []Node, state map[uint64]*VniState) error
}

type AssignSelf struct{}

func (_ AssignSelf) Assign(d *Daemon, leaderState LeaderState, _ []Node, state map[uint64]*VniState) error {
	for vni, vniState := range state {
		if vniState.Type == Unassigned {
			return d.db.NewVniUpdate(vni).Revision(vniState.Revision).LeaderState(leaderState).Type(FailoverDecided).Current("", NoLease).Next(d.Config.Node, VniLease{AttachedLeaseType, d.db.lease}).Run()
		} else if vniState.Type == Idle && vniState.Current != d.Config.Node {
			return d.db.NewVniUpdate(vni).Revision(vniState.Revision).LeaderState(leaderState).Type(MigrationDecided).Next(d.Config.Node, VniLease{AttachedLeaseType, d.db.lease}).Run()
		}
	}
	return nil
}

type AssignOther struct{}

func (_ AssignOther) Assign(d *Daemon, leaderState LeaderState, nodes []Node, state map[uint64]*VniState) error {
	node := nodes[0]
	if node.Name == d.Config.Node && len(nodes) > 1 {
		node = nodes[1]
	}
	for vni, vniState := range state {
		if vniState.Type == Idle && vniState.Current == d.Config.Node && node.Name != d.Config.Node {
			err := d.db.NewVniUpdate(vni).Revision(vniState.Revision).LeaderState(leaderState).Type(MigrationDecided).Next(node.Name, VniLease{AttachedLeaseType, node.Lease}).Run()
			if err != nil {
				return err
			}
		} else if vniState.Type == Unassigned {
			err := d.db.NewVniUpdate(vni).Revision(vniState.Revision).LeaderState(leaderState).Type(FailoverDecided).Current("", NoLease).Next(node.Name, VniLease{AttachedLeaseType, node.Lease}).Run()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type AssignGreedy struct{}

func (_ AssignGreedy) Unassigned(d *Daemon, state VniState, leaderState LeaderState, vni uint64) error {
	return nil
}

func (_ AssignGreedy) Periodic(d *Daemon, state VniState, leaderState LeaderState) error {
	return nil
}
