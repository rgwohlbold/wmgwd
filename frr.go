package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/mdlayher/arp"
	"github.com/rs/zerolog/log"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

const FRRAutonomousSystem = 65000
const MockFRR = false

var mutex sync.Mutex

type FRRClient struct {
}

func NewFRRClient() *FRRClient {
	return &FRRClient{}
}

func (frr *FRRClient) Close() {}

func (frr *FRRClient) vtysh(commands []string) error {
	if MockFRR {
		return nil
	}
	mutex.Lock()
	defer mutex.Unlock()

	input := make([]string, 2*len(commands))
	for i, c := range commands {
		input[2*i] = "-c"
		input[2*i+1] = c
	}
	log.Debug().Strs("input", input).Msg("vtysh")
	output, err := exec.Command("vtysh", input...).Output()
	log.Debug().Str("output", string(output)).Msg("vtysh")
	if err != nil {
		return err
	}
	if !strings.Contains(string(output), "[OK]\n") {
		return errors.New("vtysh returned " + string(output))
	}
	return nil
}

func (frr *FRRClient) vniToEsi(vni uint64) string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, vni)
	esi := "00:00"
	for i := 0; i < 8; i++ {
		esi += ":"
		esi += fmt.Sprintf("%02x", b[i])
	}
	return esi
}

func (frr *FRRClient) AdvertiseEvpn(vni uint64) error {
	log.Info().Uint64("vni", vni).Msg("advertising evpn")
	return frr.vtysh([]string{
		"configure terminal",
		"route-map filter-vni permit " + strconv.FormatUint(vni, 10), // vni as priority in route-map
		"match evpn vni " + strconv.FormatUint(vni, 10),
		"exit", // route-map
		"exit", // configure terminal
		"write memory",
		"clear bgp l2vpn evpn * soft",
	})
}

func (frr *FRRClient) WithdrawEvpn(vni uint64) error {
	log.Info().Uint64("vni", vni).Msg("withdrawing evpn")
	return frr.vtysh([]string{
		"configure terminal",
		"no route-map filter-vni permit " + strconv.FormatUint(vni, 10), // vni as priority in route-map
		"exit", // configure terminal
		"write memory",
		"clear bgp l2vpn evpn * soft",
	})
}

func (frr *FRRClient) AdvertiseOspf(vni uint64) error {
	log.Info().Msg("advertising ospf")
	ospfInterface := "br" + strconv.FormatUint(vni, 10)
	return frr.vtysh([]string{
		"configure terminal",
		"interface " + ospfInterface,
		"no ospf cost",
		"exit", // interface,
		"exit", // configure terminal
		"write memory",
	})
}

func (frr *FRRClient) WithdrawOspf(vni uint64) error {
	log.Info().Msg("withdrawing ospf")
	ospfInterface := "br" + strconv.FormatUint(vni, 10)
	return frr.vtysh([]string{
		"configure terminal",
		"interface " + ospfInterface,
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

func (frr *FRRClient) EnableArp(vni uint64) error {
	return writeSysctl("net.ipv4.conf.br"+strconv.FormatUint(vni, 10)+".arp_ignore", "0")
}

func (frr *FRRClient) DisableArp(vni uint64) error {
	return writeSysctl("net.ipv4.conf.br"+strconv.FormatUint(vni, 10)+".arp_ignore", "3")
}

func (frr *FRRClient) SendGratuitousArp(vni uint64) error {
	log.Info().Uint64("vni", vni).Msg("sending gratuitous arp")
	// TODO: this blocks
	if MockFRR {
		return nil
	}
	iface, err := net.InterfaceByName("br" + strconv.FormatUint(vni, 10))
	if err != nil {
		return err
	}
	client, err := arp.Dial(iface)
	if err != nil {
		return err
	}
	defer client.Close()

	addrs, err := iface.Addrs()
	if err != nil {
		return err
	}
	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err != nil {
			return err
		}
		if ip.To4() != nil {
			nip, ok := netip.AddrFromSlice(ip.To4())
			if !ok {
				return errors.New("failed to convert ip to netip")
			}
			packet, err := arp.NewPacket(arp.OperationReply, iface.HardwareAddr, nip, net.HardwareAddr{0, 0, 0, 0, 0, 0}, nip)
			if err != nil {
				return err
			}
			err = client.WriteTo(packet, net.HardwareAddr{0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
