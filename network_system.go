package main

import (
	"github.com/mdlayher/arp"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

type SystemNetworkStrategy struct {
	lock *sync.Mutex
}

func NewSystemNetworkStrategy() *SystemNetworkStrategy {
	return &SystemNetworkStrategy{
		lock: &sync.Mutex{},
	}
}

func (s *SystemNetworkStrategy) bridgeName(vni uint64) string {
	return "br" + strconv.FormatUint(vni, 10)
}

func (s *SystemNetworkStrategy) vtysh(commands []string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	input := make([]string, 2*len(commands))
	for i, c := range commands {
		input[2*i] = "-c"
		input[2*i+1] = c
	}
	log.Debug().Strs("input", input).Msg("vtysh")
	output, err := exec.Command("vtysh", input...).Output()
	log.Debug().Str("output", string(output)).Msg("vtysh")
	if err != nil {
		return errors.Wrap(err, "vtysh failed")
	}
	if !strings.Contains(string(output), "[OK]\n") {
		return errors.New("vtysh returned " + string(output))
	}
	return nil
}

func (s *SystemNetworkStrategy) AdvertiseEvpn(vni uint64) error {
	log.Debug().Uint64("vni", vni).Msg("advertising evpn")
	return s.vtysh([]string{
		"configure terminal",
		"route-map filter-vni permit " + strconv.FormatUint(vni, 10), // vni as priority in route-map
		"match evpn vni " + strconv.FormatUint(vni, 10),
		"exit", // route-map
		"exit", // configure terminal
		"write memory",
		"clear bgp l2vpn evpn * soft",
	})
}

func (s *SystemNetworkStrategy) WithdrawEvpn(vni uint64) error {
	log.Debug().Uint64("vni", vni).Msg("withdrawing evpn")
	return s.vtysh([]string{
		"configure terminal",
		"no route-map filter-vni permit " + strconv.FormatUint(vni, 10), // vni as priority in route-map
		"exit", // configure terminal
		"write memory",
		"clear bgp l2vpn evpn * soft",
	})
}

func (s *SystemNetworkStrategy) AdvertiseOspf(vni uint64) error {
	log.Debug().Msg("advertising ospf")
	return s.vtysh([]string{
		"configure terminal",
		"interface " + s.bridgeName(vni),
		"no ospf cost",
		"exit", // interface,
		"exit", // configure terminal
		"write memory",
	})
}

func (s *SystemNetworkStrategy) WithdrawOspf(vni uint64) error {
	log.Debug().Msg("withdrawing ospf")
	return s.vtysh([]string{
		"configure terminal",
		"interface " + s.bridgeName(vni),
		"ospf cost 65535",
		"exit", // interface,
		"exit", // configure terminal
		"write memory",
	})
}

func writeSysctl(key string, value string) error {
	path := strings.Replace(key, ".", "/", -1)
	return os.WriteFile("/proc/sys/"+path, []byte(value), 0644)
}

func (s *SystemNetworkStrategy) EnableArp(vni uint64) error {
	return writeSysctl("net.ipv4.conf."+s.bridgeName(vni)+".arp_ignore", "0")
}

func (s *SystemNetworkStrategy) DisableArp(vni uint64) error {
	return writeSysctl("net.ipv4.conf."+s.bridgeName(vni)+".arp_ignore", "3")
}

func (s *SystemNetworkStrategy) SendGratuitousArp(vni uint64) error {
	log.Debug().Uint64("vni", vni).Msg("sending gratuitous arp")
	iface, err := net.InterfaceByName(s.bridgeName(vni))
	if err != nil {
		return errors.Wrap(err, "could not get interface")
	}
	client, err := arp.Dial(iface)
	if err != nil {
		return errors.Wrap(err, "could not dial arp")
	}
	defer client.Close()

	addrs, err := iface.Addrs()
	if err != nil {
		return errors.Wrap(err, "could not get interface addresses")
	}
	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err != nil {
			return errors.Wrap(err, "could not parse cidr")
		}
		if ip.To4() != nil {
			nip, ok := netip.AddrFromSlice(ip.To4())
			if !ok {
				return errors.New("failed to convert ip to netip")
			}
			packet, err := arp.NewPacket(arp.OperationReply, iface.HardwareAddr, nip, net.HardwareAddr{0, 0, 0, 0, 0, 0}, nip)
			if err != nil {
				return errors.Wrap(err, "could not create arp packet")
			}
			err = client.WriteTo(packet, net.HardwareAddr{0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
			if err != nil {
				return errors.Wrap(err, "could not write arp packet")
			}
		}
	}
	return nil
}

func (s *SystemNetworkStrategy) ByteCounter(vni uint64) (uint64, error) {
	res, err := os.ReadFile("/sys/class/net/" + s.bridgeName(vni) + "/statistics/tx_bytes")
	if err != nil {
		return 0, errors.Wrap(err, "could not read tx_bytes")
	}
	return strconv.ParseUint(strings.TrimSpace(string(res)), 10, 64)
}
