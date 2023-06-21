package main

type OspfCost uint16

const (
	OspfIdleCost     OspfCost = 100
	OspfWithdrawCost OspfCost = 65535
)

type NetworkStrategy interface {
	AdvertiseEvpn(vni uint64) error
	WithdrawEvpn(vni uint64) error
	AdvertiseOspf(vni uint64, cost OspfCost) error
	EnableArp(vni uint64) error
	DisableArp(vni uint64) error
	SendGratuitousArp(vni uint64) error
}
