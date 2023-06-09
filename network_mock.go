package main

import "sync"

type MockNetworkStrategy struct {
	ospfAdvertised map[uint64]bool
	evpnAdvertised map[uint64]bool
	arpEnabled     map[uint64]bool
	gratuitousArp  map[uint64]int
	lastBytes      *uint64
	mutex          *sync.Mutex
}

func NewMockNetworkStrategy() *MockNetworkStrategy {
	return &MockNetworkStrategy{
		ospfAdvertised: make(map[uint64]bool),
		evpnAdvertised: make(map[uint64]bool),
		arpEnabled:     make(map[uint64]bool),
		gratuitousArp:  make(map[uint64]int),
		lastBytes:      new(uint64),
		mutex:          &sync.Mutex{},
	}
}

func (s MockNetworkStrategy) AdvertiseOspf(vni uint64, cost OspfCost) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if cost == OspfWithdrawCost {
		s.ospfAdvertised[vni] = false
	} else {
		s.ospfAdvertised[vni] = true
	}
	return nil
}

func (s MockNetworkStrategy) WithdrawOspf(vni uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.ospfAdvertised[vni] = false
	return nil
}

func (s MockNetworkStrategy) AdvertiseEvpn(vni uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.evpnAdvertised[vni] = true
	return nil
}

func (s MockNetworkStrategy) WithdrawEvpn(vni uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.evpnAdvertised[vni] = false
	return nil
}

func (s MockNetworkStrategy) EnableArp(vni uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.arpEnabled[vni] = true
	return nil
}

func (s MockNetworkStrategy) DisableArp(vni uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.arpEnabled[vni] = false
	return nil
}

func (s MockNetworkStrategy) SendGratuitousArp(vni uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.gratuitousArp[vni] += 1
	return nil
}
