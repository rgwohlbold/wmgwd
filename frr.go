package main

import (
	"errors"
	"github.com/rs/zerolog/log"
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

func (frr *FRRClient) vtysh(commands []string) ([]byte, error) {
	mutex.Lock()
	defer mutex.Unlock()

	input := make([]string, 2*len(commands))
	for i, c := range commands {
		input[2*i] = "-c"
		input[2*i+1] = c
	}
	return exec.Command("vtysh", input...).Output()
}

func (frr *FRRClient) Advertise(vni int) error {
	log.Info().Int("vni", vni).Msg("advertising")
	if MockFRR {
		return nil
	}
	iface := "veth" + strconv.Itoa(vni) + "p"
	out, err := frr.vtysh([]string{
		"configure terminal",
		"interface bond100",
		"evpn mh es-id 00:00:00:00:00:00:00:00:00:01",
		"exit", // interface bond100
		"interface " + iface,
		"no ospf cost",
		"exit", // interface,
		"router bgp " + strconv.Itoa(FRRAutonomousSystem),
		"address-family l2vpn evpn",
		"vni " + strconv.Itoa(vni),
		"advertise-svi-ip",
		"exit", // vni
		"exit", // address-family
		"exit", // router bgp
		"exit", // configure terminal
		"write memory",
	})
	if err != nil {
		return err
	}
	if !strings.Contains(string(out), "[OK]\n") {
		return errors.New("vtysh returned " + string(out))
	}
	return nil
}

func (frr *FRRClient) Withdraw(vni int) error {
	log.Info().Int("vni", vni).Msg("withdrawing")
	if MockFRR {
		return nil
	}
	iface := "veth" + strconv.Itoa(vni) + "p"
	out, err := frr.vtysh([]string{
		"configure terminal",
		"interface bond100",
		"no evpn mh es-id",
		"exit", // interface bond100
		"interface " + iface,
		"ospf cost 65535",
		"exit", // interface,
		"router bgp " + strconv.Itoa(FRRAutonomousSystem),
		"address-family l2vpn evpn",
		"vni " + strconv.Itoa(vni),
		"no advertise-svi-ip",
		"exit", // vni
		"exit", // address-family
		"exit", // router bgp
		"exit", // configure terminal
		"write memory",
	})
	if err != nil {
		return err
	}
	if !strings.Contains(string(out), "[OK]\n") {
		return errors.New("vtysh returned " + string(out))
	}
	return nil

}
