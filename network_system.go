package main

import (
	"context"
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

type FrrCommandType int

const MaxCommands = 100
const RouteMapPriorityOffset = 10

const (
	FrrOspfAdvertise FrrCommandType = iota
	FrrEvpnAdvertise
	FrrEvpnWithdraw
)

type FrrCommand struct {
	Type         FrrCommandType
	Vni          uint64
	ResponseChan chan error
	Cost         OspfCost
}

type SystemNetworkStrategy struct {
	vtyshLock        sync.Mutex
	commandLock      sync.Mutex
	commands         []FrrCommand
	updatesAvailable chan struct{}
}

func NewSystemNetworkStrategy() *SystemNetworkStrategy {
	return &SystemNetworkStrategy{
		vtyshLock:        sync.Mutex{},
		commandLock:      sync.Mutex{},
		updatesAvailable: make(chan struct{}, 1),
	}
}

func (s *SystemNetworkStrategy) bridgeName(vni uint64) string {
	return "br" + strconv.FormatUint(vni, 10)
}

func (s *SystemNetworkStrategy) vtysh(commands []FrrCommand) error {
	s.vtyshLock.Lock()
	defer s.vtyshLock.Unlock()

	cmd := exec.Command("vtysh")
	stdinPipe, err := cmd.StdinPipe()
	defer stdinPipe.Close()
	if err != nil {
		return errors.Wrap(err, "vtysh failed: stdin pipe open failed")
	}
	_, err = stdinPipe.Write([]byte("configure terminal\n"))
	if err != nil {
		return errors.Wrap(err, "vtysh failed: write to stdin failed")
	}
	clear := false
	for _, command := range commands {
		var bytes []byte
		if command.Type == FrrOspfAdvertise {
			bytes = []byte("interface " + s.bridgeName(command.Vni) + "\n" +
				"ospf cost " + strconv.FormatUint(uint64(command.Cost), 10) + "\n" +
				"exit\n")
		} else if command.Type == FrrEvpnAdvertise {
			bytes = []byte("route-map filter-vni permit " + strconv.FormatUint(command.Vni+RouteMapPriorityOffset, 10) + "\n" +
				"match evpn vni " + strconv.FormatUint(command.Vni, 10) + "\n" +
				"exit\n")
			clear = true
		} else if command.Type == FrrEvpnWithdraw {
			bytes = []byte("no route-map filter-vni permit " + strconv.FormatUint(command.Vni+RouteMapPriorityOffset, 10) + "\n")
			clear = true
		}
		_, err = stdinPipe.Write(bytes)
		if err != nil {
			return errors.Wrap(err, "vtysh failed: write to stdin failed")
		}
	}
	_, err = stdinPipe.Write([]byte("exit\nwrite memory\n"))
	if err != nil {
		return errors.Wrap(err, "vtysh failed: write to stdin failed")
	}
	if clear {
		_, err = stdinPipe.Write([]byte("clear bgp l2vpn evpn * soft\n"))
		if err != nil {
			return errors.Wrap(err, "vtysh failed: write to stdin failed")
		}
	}
	_, err = stdinPipe.Write([]byte("exit\n"))
	err = stdinPipe.Close()
	if err != nil {
		return errors.Wrap(err, "vtysh failed: stdin pipe close failed")
	}
	log.Debug().Msg("waiting for vtysh output")
	output, err := cmd.Output()
	log.Debug().Str("output", string(output)).Msg("vtysh")
	if err != nil {
		return errors.Wrap(err, "vtysh failed")
	}
	if !strings.Contains(string(output), "[OK]\n") {
		return errors.New("vtysh returned " + string(output))
	}
	return nil
}

func (s *SystemNetworkStrategy) Loop(ctx context.Context) {
	for {
	start:
		select {
		case <-ctx.Done():
			return
		case <-s.updatesAvailable:
			for {
				s.commandLock.Lock()
				num := len(s.commands)
				if num > MaxCommands {
					num = MaxCommands
				}
				commands := s.commands[:num]
				s.commands = s.commands[num:]
				s.commandLock.Unlock()
				if num == 0 {
					goto start
				}
				log.Debug().Int("commands", len(commands)).Msg("processing commands")
				err := s.vtysh(commands)
				for _, command := range commands {
					command.ResponseChan <- err
				}
			}
		}
	}
}

func (s *SystemNetworkStrategy) WaitForCommand(command FrrCommand) error {
	command.ResponseChan = make(chan error, 1)
	s.commandLock.Lock()
	s.commands = append(s.commands, command)
	if len(s.updatesAvailable) == 0 {
		s.updatesAvailable <- struct{}{}
	}
	s.commandLock.Unlock()
	return <-command.ResponseChan
}

func (s *SystemNetworkStrategy) AdvertiseEvpn(vni uint64) error {
	log.Debug().Uint64("vni", vni).Msg("advertising evpn")
	return s.WaitForCommand(FrrCommand{
		Type: FrrEvpnAdvertise,
		Vni:  vni,
	})
}

func (s *SystemNetworkStrategy) WithdrawEvpn(vni uint64) error {
	log.Debug().Uint64("vni", vni).Msg("withdrawing evpn")
	return s.WaitForCommand(FrrCommand{
		Type: FrrEvpnWithdraw,
		Vni:  vni,
	})
}

func (s *SystemNetworkStrategy) AdvertiseOspf(vni uint64, cost OspfCost) error {
	log.Debug().Uint64("vni", vni).Msg("advertising ospf")
	return s.WaitForCommand(FrrCommand{
		Type: FrrOspfAdvertise,
		Vni:  vni,
		Cost: cost,
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
