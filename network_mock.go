package main

type MockNetworkStrategy struct {
	numOspfAdvertised map[uint64]int
	numOspfWithdrawn  map[uint64]int
	numEvpnAdvertised map[uint64]int
	numEvpnWithdrawn  map[uint64]int
	numArpEnabled     map[uint64]int
	numArpDisabled    map[uint64]int
	numGratuitousArp  map[uint64]int
}

func NewMockNetworkStrategy() *MockNetworkStrategy {
	return &MockNetworkStrategy{
		numOspfAdvertised: make(map[uint64]int),
		numOspfWithdrawn:  make(map[uint64]int),
		numEvpnAdvertised: make(map[uint64]int),
		numEvpnWithdrawn:  make(map[uint64]int),
		numArpEnabled:     make(map[uint64]int),
		numArpDisabled:    make(map[uint64]int),
		numGratuitousArp:  make(map[uint64]int),
	}
}

func (s *MockNetworkStrategy) AdvertiseOspf(vni uint64) error {
	s.numOspfAdvertised[vni]++
	return nil
}

func (s *MockNetworkStrategy) WithdrawOspf(vni uint64) error {
	s.numOspfWithdrawn[vni]++
	return nil
}

func (s *MockNetworkStrategy) AdvertiseEvpn(vni uint64) error {
	s.numEvpnAdvertised[vni]++
	return nil
}

func (s *MockNetworkStrategy) WithdrawEvpn(vni uint64) error {
	s.numEvpnWithdrawn[vni]++
	return nil
}

func (s *MockNetworkStrategy) EnableArp(vni uint64) error {
	s.numArpEnabled[vni]++
	return nil
}

func (s *MockNetworkStrategy) DisableArp(vni uint64) error {
	s.numArpDisabled[vni]++
	return nil
}

func (s *MockNetworkStrategy) SendGratuitousArp(vni uint64) error {
	s.numGratuitousArp[vni]++
	return nil
}
