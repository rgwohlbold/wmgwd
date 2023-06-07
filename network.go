package main

type NetworkStrategy interface {
	AdvertiseEvpn(vni uint64) error
	WithdrawEvpn(vni uint64) error
	AdvertiseOspf(vni uint64) error
	WithdrawOspf(vni uint64) error
	EnableArp(vni uint64) error
	DisableArp(vni uint64) error
	SendGratuitousArp(vni uint64) error
}
