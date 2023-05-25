package main

type MockNetworkStrategy struct {
	ospfAdvertised map[uint64]bool
	evpnAdvertised map[uint64]bool
	arpEnabled     map[uint64]bool
	gratuitousArp  map[uint64]int
}

func NewMockNetworkStrategy() *MockNetworkStrategy {
	return &MockNetworkStrategy{
		ospfAdvertised: make(map[uint64]bool),
		evpnAdvertised: make(map[uint64]bool),
		arpEnabled:     make(map[uint64]bool),
		gratuitousArp:  make(map[uint64]int),
	}
}

func (s MockNetworkStrategy) AdvertiseOspf(vni uint64) error {
	s.ospfAdvertised[vni] = true
	return nil
}

func (s MockNetworkStrategy) WithdrawOspf(vni uint64) error {
	s.ospfAdvertised[vni] = false
	return nil
}

func (s MockNetworkStrategy) AdvertiseEvpn(vni uint64) error {
	s.evpnAdvertised[vni] = true
	return nil
}

func (s MockNetworkStrategy) WithdrawEvpn(vni uint64) error {
	s.evpnAdvertised[vni] = false
	return nil
}

func (s MockNetworkStrategy) EnableArp(vni uint64) error {
	s.arpEnabled[vni] = true
	return nil
}

func (s MockNetworkStrategy) DisableArp(vni uint64) error {
	s.arpEnabled[vni] = false
	return nil
}

func (s MockNetworkStrategy) SendGratuitousArp(vni uint64) error {
	s.gratuitousArp[vni] += 1
	return nil
}

func (s MockNetworkStrategy) ByteCounter(_ uint64) (uint64, error) {
	return 5, nil
}
