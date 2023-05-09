package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"math/rand"
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
	log.Debug().Strs("input", input).Msg("vtysh")
	output, err := exec.Command("vtysh", input...).Output()
	if err != nil {
		return output, err
	}
	log.Debug().Str("output", string(output)).Msg("vtysh")
	return output, err
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

func (frr *FRRClient) Advertise(vni uint64) error {
	log.Info().Uint64("vni", vni).Msg("advertising")
	if MockFRR {
		return nil
	}
	ospfInterface := "veth" + strconv.FormatUint(vni, 10) + "p"
	bondInterface := "bond" + strconv.FormatUint(vni, 10)
	out, err := frr.vtysh([]string{
		"configure terminal",
		"interface " + bondInterface,
		"evpn mh es-id " + frr.vniToEsi(vni),
		"evpn mh es-df-pref " + strconv.Itoa(rand.Intn(32767)),
		"exit", // interface bond100
		"interface " + ospfInterface,
		"no ospf cost",
		"exit", // interface,
		"router bgp " + strconv.Itoa(FRRAutonomousSystem),
		"address-family l2vpn evpn",
		"vni " + strconv.FormatUint(vni, 10),
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

func (frr *FRRClient) Withdraw(vni uint64) error {
	log.Info().Uint64("vni", vni).Msg("withdrawing")
	if MockFRR {
		return nil
	}
	ospfInterface := "veth" + strconv.FormatUint(vni, 10) + "p"
	bondInterface := "bond" + strconv.FormatUint(vni, 10)
	out, err := frr.vtysh([]string{
		"configure terminal",
		"interface " + bondInterface,
		"no evpn mh es-id",
		"no evpn mh es-df-pref",
		"exit", // interface bond100
		"interface " + ospfInterface,
		"ospf cost 65535",
		"exit", // interface,
		"router bgp " + strconv.Itoa(FRRAutonomousSystem),
		"address-family l2vpn evpn",
		"vni " + strconv.FormatUint(vni, 10),
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
