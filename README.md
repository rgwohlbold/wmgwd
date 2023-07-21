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
