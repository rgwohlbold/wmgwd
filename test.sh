#!/bin/sh

[ -d "default.etcd" ] && rm -rf default.etcd
killall etcd
~/progs/etcd/bin/etcd >/dev/null 2>&1 &
sleep 1
go test
killall etcd
