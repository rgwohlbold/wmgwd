# wmgwd

`wmgwd` is a distributed gateway daemon for WiMoVE.
I wrote this daemon as part of my bachelor's thesis.

## Architecture

The idea is to run an instance of [etcd](https://etcd.io/) on every gateway.
`wmgwd` connects to the local database instance.
The daemon uses the database for three things:

1. _Peer discovery_: Every instance of `wmgwd` writes its name to a key, notifying all other instances.
2. _Failure detection_: `wmgwd` uses [time-bound leases](https://martinfowler.com/articles/patterns-of-distributed-systems/time-bound-lease.html). When a lease expires, all other instances recognize the failure.  
3. _Virtual gateway management_: The state of each overlay network is stored in the database.
  The daemon implements a state machine for each overlay network on top of the database.

To assign overlay networks to gateways, `wmgwd` uses [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing) to randomly assign overlay networks to physical gateways.

## Host System Interaction

`wmgwd` enables and disables ARP on selected interfaces via the [`/proc` filesystem](https://docs.kernel.org/filesystems/proc.html). 
The daemon also reconfigures FRR via its command-line interface `vtysh`.
Advertising and withdrawing BGP EVPN routes is done by configuring route-maps at runtime.
Advertising and withdrawing OSPF routes is done by setting link costs to 100 or 65535. 

## Build

```bash
git clone https://github.com/rgwohlbold/wmgwd
cd wmgwd
go build
```

This creates an executable `wmgwd`.

## Configuration

| Name   | Default               | Explanation                                                                |
|--------|-----------------------|----------------------------------------------------------------------------|
| name   |                       | Node name in etcd                                                          |
| minvni | 1                     | Minimum VXLAN Network Identifier                                           |
| maxvni | 100                   | Maximum VXLAN Network Identifier                                           |
| mock   | false                 | Do not interact with host network configuration and FRR                    |
| uids   |                       | Comma-separated list of UIDs used for consistent hashing (default: random) |
| debug  | false                 | Enable debug logging                                                       |
| etcd   | http://localhost:2379 | Etcd endpoint                                                              |

## Network Configuration

For each VNI `i`, `wmgwd` assumes two interfaces: `vxlan$i` and `br$i` where the `vxlan$i` has `br$i` as its master.
`br$i` is assigned the IP addresses that all devices in that overlay network use as their default gateway.

`wmgwd` assumes that FRR is run on the gateway and announces routes with BGP EVPN into WiMoVE and with OSPF into the rest of the network.
The daemon restricts which BGP EVPN routes are advertised by FRR using route maps.

There, a route map called `filter-vni` that permits all BGP EVPN routes that are not of type 2 (MAC/IP advertisement route) or type 5 (IP prefix route) is required.
This is an example configuration file of FRR that works with `wmgwd`:

```
ip forwarding
ip nht resolve-via-default
ip6 nht resolve-via-default
router bgp 65000
  bgp router-id 10.0.1.11
  no bgp default ipv4-unicast
  neighbor fabric peer-group
  neighbor fabric remote-as 65000
  neighbor fabric capability extended-nexthop
  neighbor fabric ebgp-multihop 5
  neighbor fabric timers 1 3
  neighbor 10.0.1.17 peer-group fabric
  address-family l2vpn evpn
   neighbor fabric activate
   neighbor fabric route-map filter-vni out
   advertise-all-vni
   advertise-svi-ip
  exit-address-family
  !
!
router ospf
 ospf router-id 1.1.1.1
 network 0.0.0.0/0 area 0.0.0.0
!
route-map filter-vni permit 1
 match evpn route-type 2
 on-match goto 4
!
route-map filter-vni permit 2
 match evpn route-type 5
 on-match goto 4
!
route-map filter-vni permit 3
!
```
