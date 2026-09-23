#!/bin/bash
# fio stage on slice barkovbg, runs on cloud. Env: SIDE=base|cand BINARY=main|pr SHA=<sha> SCENARIO=quiet|slow DISK=disk1
set -u
: "${SIDE:?}" "${BINARY:?}" "${SHA:?}" "${SCENARIO:?}" "${DISK:?}"
ROOT=~/nbs2/ydb-fio-$SIDE
EP=vla5-8296.search.yandex.net
HOSTS="vla5-8296 vla5-8270 vla5-8257 vla5-8267 vla5-8284 vla5-8283 vla5-8248 vla5-8263"
SERIAL=/tmp/fio-$SIDE-$SCENARIO.serial.log
NETEM_DELAY=${NETEM_DELAY:-10ms}
echo "== fio stage: side=$SIDE binary=$BINARY sha=$SHA scenario=$SCENARIO disk=$DISK root=$ROOT"
grep -q 'PROMOTE_EXIT:0' ~/nbs2/logs/promote-barkovbg-$SIDE.log || { echo "!! promote of $SIDE not finished with 0"; exit 1; }
[ "$(git -C $ROOT rev-parse --short=11 HEAD)" = "$SHA" ] || { echo "!! $ROOT is not at $SHA"; exit 1; }
YDBD=$ROOT/ydb/apps/ydbd/ydbd
DSTOOL=$ROOT/ydb/apps/dstool/ydb-dstool
[ -x "$DSTOOL" ] || { echo "== building dstool"; (cd $ROOT && ya make -r ydb/apps/dstool 2>&1 | tail -2); }
[ -x "$DSTOOL" ] || { echo "!! no dstool"; exit 1; }
SSHO="-o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=no"

# ---- slow host: netem on one PB host (not the endpoint host)
SLOW=""
if [ "$SCENARIO" = slow ]; then
  for h in $HOSTS; do [ "$h" != vla5-8296 ] && { SLOW=$h; break; }; done
  IF=$(ssh $SSHO barkovbg@$SLOW.search.yandex.net "ip -6 route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if(\$i==\"dev\") print \$(i+1)}' | head -1; ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if(\$i==\"dev\") print \$(i+1)}' | head -1" | head -1)
  echo "== slow host $SLOW iface $IF delay $NETEM_DELAY"
  ssh $SSHO barkovbg@$SLOW.search.yandex.net "sudo tc qdisc del dev $IF root 2>/dev/null; sudo tc qdisc add dev $IF root netem delay $NETEM_DELAY && sudo tc qdisc show dev $IF"
fi
cleanup_netem() { [ -n "$SLOW" ] && ssh $SSHO barkovbg@$SLOW.search.yandex.net "sudo tc qdisc del dev $IF root; sudo tc qdisc show dev $IF" ; }

# ---- pool + disk
echo "== pool"; $YDBD -s $EP:2135 admin bs config invoke --proto 'Command { DefineDDiskPool { BoxId: 1 Name: "ddp1" Geometry { NumFailRealms: 1 RealmLevelBegin: 10 RealmLevelEnd: 10 DomainLevelBegin: 10 DomainLevelEnd: 40 NumFailDomainsPerFailRealm: 5 NumVDisksPerFailDomain: 1} PDiskFilter { Property { Type: SSD } } NumDDiskGroups: 3 } }' 2>&1 | tail -3
echo "== disk $DISK"; $DSTOOL -d -e grpc://$EP:2135 nbs partition create --block-size 4096 --blocks-count 1048576 --pool ddp1 --type=ssd --disk-id $DISK 2>&1 | tail -3
sleep 5
AID1=$($DSTOOL -d -e grpc://$EP:2135 nbs partition get-load-actor-adapter-actor-id --disk-id $DISK 2>&1 | tail -1); echo "actorId before: $AID1"

# ---- socket host
Q=""
for try in $(seq 1 12); do
  for h in $HOSTS; do
    if ssh $SSHO barkovbg@$h.search.yandex.net "test -S /tmp/$DISK.sock" 2>/dev/null; then Q=$h; break; fi
  done
  [ -n "$Q" ] && break; sleep 5
done
[ -n "$Q" ] || { echo "!! no /tmp/$DISK.sock on any host"; cleanup_netem; exit 1; }
echo "== qemu host: $Q"
SSHQ="ssh $SSHO barkovbg@$Q.search.yandex.net"

# ---- qemu + serial capture
$SSHQ "tmux kill-session -t nbs-qemu 2>/dev/null; rm -f $SERIAL; tmux new-session -d -s nbs-qemu -c /home/barkovbg 'sudo ./run_qemu.sh -d $DISK'; sleep 1; tmux pipe-pane -t nbs-qemu -o 'cat >> $SERIAL'; echo qemu started"
wait_serial() { local pat=$1 n=$2 i; for i in $(seq 1 $n); do $SSHQ "grep -q -a -- '$pat' $SERIAL" 2>/dev/null && return 0; sleep 5; done; return 1; }
wait_serial 'ubuntu login:' 36 || { echo "!! no login prompt"; $SSHQ "tail -c 1500 $SERIAL"; cleanup_netem; exit 1; }
$SSHQ "tmux send-keys -t nbs-qemu qemu Enter"; sleep 2
wait_serial 'Password:' 12 || echo "?? no password prompt yet"
$SSHQ "tmux send-keys -t nbs-qemu qemupass Enter"; sleep 3
wait_serial 'qemu@' 24 || { echo "!! no shell"; $SSHQ "tail -c 1500 $SERIAL"; cleanup_netem; exit 1; }
echo "== guest shell ok"

# ---- deliver the campaign script in <2000-char chunks (serial canonical line limit)
$SSHQ "tmux send-keys -t nbs-qemu 'rm -f /tmp/g.b64' Enter"
G64=$(base64 -w0 ~/nbs2/guest_fio_campaign.sh)
for ((o=0; o<${#G64}; o+=1800)); do
  chunk=${G64:o:1800}
  $SSHQ "tmux send-keys -t nbs-qemu 'echo $chunk >> /tmp/g.b64' Enter"; sleep 1
done
$SSHQ "tmux send-keys -t nbs-qemu 'base64 -d /tmp/g.b64 > /tmp/guest_fio_campaign.sh && echo SCRIPT_OK \$(wc -c < /tmp/guest_fio_campaign.sh)' Enter"
wait_serial 'SCRIPT_OK' 12 || { echo "!! script delivery failed"; $SSHQ "tail -c 1500 $SERIAL"; cleanup_netem; exit 1; }

# ---- run campaign
$SSHQ "tmux send-keys -t nbs-qemu 'bash /tmp/guest_fio_campaign.sh' Enter"
echo "== campaign started $(date -u +%H:%M:%S)"
wait_serial 'CAMPAIGN_DONE' 360 || echo "!! campaign timeout (30 min)"
echo "== campaign finished $(date -u +%H:%M:%S)"

# ---- collect
AID2=$($DSTOOL -d -e grpc://$EP:2135 nbs partition get-load-actor-adapter-actor-id --disk-id $DISK 2>&1 | tail -1); echo "actorId after:  $AID2"
scp $SSHO barkovbg@$Q.search.yandex.net:$SERIAL ~/nbs2/logs/fio-$SIDE-$SCENARIO.serial.log
python3 ~/nbs2/parse_fio.py --binary $BINARY --sha $SHA --actor-id "$AID2" ~/nbs2/logs/fio-$SIDE-$SCENARIO.serial.log >> ~/nbs2/logs/fio-ab-$SCENARIO.jsonl
echo "== FIO_JSONL lines: $(grep -a -c FIO_JSONL ~/nbs2/logs/fio-$SIDE-$SCENARIO.serial.log)"
grep -a FIO_JSONL ~/nbs2/logs/fio-$SIDE-$SCENARIO.serial.log | sed 's/^.*FIO_JSONL //' | python3 -c '
import sys,json
print("job | rep | rw | err | drop/short | IOPS | BW MB/s | p50 us | p99 us")
for l in sys.stdin:
    try: r=json.loads(l)
    except Exception: continue
    print(f"{r[\"job\"]} | {r[\"repeat\"]} | {r[\"rw\"]} | {r[\"err\"]} | {r[\"dropped\"]}/{r[\"short\"]} | {r[\"iops\"]:.0f} | {r[\"bw_bytes\"]/1e6:.1f} | {(r[\"lat_ns_p50\"] or 0)/1000:.0f} | {(r[\"lat_ns_p99\"] or 0)/1000:.0f}")'
echo "== errors in serial"; grep -a -c -i 'verify\|error' ~/nbs2/logs/fio-$SIDE-$SCENARIO.serial.log

# ---- persist transactions on every host (log discovery)
echo "== persist counters per host"
for h in $HOSTS; do
  ssh $SSHO barkovbg@$h.search.yandex.net 'f=$(ls -t $HOME/multinode_home/*/*.log $HOME/multinode_home/*/logs/*.log 2>/dev/null | head -2); [ -n "$f" ] && { echo "$(hostname): $f"; grep -a -c "Will persist dirty map" $f; } || { echo "$(hostname): no log found; $(ls $HOME/multinode_home 2>/dev/null | tr "\n" " ")"; }' 2>/dev/null
done

# ---- teardown
$SSHQ "tmux kill-session -t nbs-qemu 2>/dev/null; echo qemu killed"
cleanup_netem
echo "== stage done"
