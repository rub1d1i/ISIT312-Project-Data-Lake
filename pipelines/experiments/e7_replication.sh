#!/bin/bash
# ---------------------------------------------------------------------------
# E7  Replication and the loss of a DataNode (NFR2)
#
# The prototype has one DataNode, so every file has one replica. This test adds
# three more DataNode processes on the same machine (four in all), stores one
# file with three replicas, stops a DataNode that holds a replica and checks that
#   - the file is still read completely while the node is gone,
#   - the NameNode makes a new third replica on the remaining nodes by itself.
# It shows how HDFS behaves when a node is lost. All four DataNodes share one
# machine, so it says nothing about speed and nothing about losing a machine.
#
# Run inside the lake container with HBase stopped:  bash e7_replication.sh
# ---------------------------------------------------------------------------
set -uo pipefail
SRC=/lake/curated/media/recordings_packed/branch=A/dt=2026-11-16/recordings.seq
DIR=/lake/tmp/replication_test
F=$DIR/recordings.seq
live() { hdfs dfsadmin -report -live 2>/dev/null | grep -c '^Name: '; }
dead() { hdfs dfsadmin -report -dead 2>/dev/null | grep -c '^Name: '; }
replicas() { hdfs fsck $F -files -blocks -locations 2>/dev/null | grep -E '^[0-9]+\. BP-' | sed -E 's/BP-[^:]+:(blk_[0-9]+)_[0-9]+/\1/; s/DatanodeInfoWithStorage\[([0-9.:]+),[^]]*\]/\1/g'; }
summary() { hdfs fsck $F 2>/dev/null | grep -E 'Total blocks|Minimally replicated|Under-replicated|Missing replicas|Average block replication|Number of data-nodes|is HEALTHY|is CORRUPT'; }

echo "== 1. three more DataNode processes on this machine"
for i in 2 3 4; do
  mkdir -p /tmp/dn$i
  nohup hdfs datanode -Ddfs.datanode.data.dir=file:///tmp/dn$i/data -Ddfs.datanode.address=0.0.0.0:5${i}010 \
        -Ddfs.datanode.http.address=0.0.0.0:5${i}075 -Ddfs.datanode.ipc.address=0.0.0.0:5${i}020 > /tmp/dn$i/datanode.log 2>&1 &
  echo $! > /tmp/dn$i/pid
done
for n in $(seq 1 60); do [ "$(live)" = 4 ] && break; sleep 2; done
echo "live DataNodes: $(live)"
hdfs dfsadmin -report 2>/dev/null | grep '^Name: '

echo
echo "== 2. one day of packed recordings stored with three replicas"
hadoop fs -rm -r -f -skipTrash $DIR > /dev/null 2>&1; hadoop fs -mkdir -p $DIR 2> /dev/null
hadoop fs -D dfs.replication=3 -cp $SRC $F 2> /dev/null
hadoop fs -ls $F 2> /dev/null
MD5_BEFORE=$(hadoop fs -cat $F 2> /dev/null | md5sum | cut -d' ' -f1)
echo "md5 of the file: $MD5_BEFORE"
replicas; summary

VICTIM_PORT=$(replicas | head -1 | grep -oE '5[234]010' | head -1)
i=${VICTIM_PORT:1:1}
echo
echo "== 3. the DataNode on port $VICTIM_PORT holds a replica of the first block: stop it ($(date +%H:%M:%S))"
{ kill -9 "$(cat /tmp/dn$i/pid)"; wait "$(cat /tmp/dn$i/pid)"; } 2> /dev/null; sleep 3
T0=$(date +%s)
MD5_DURING=$(hadoop fs -cat $F 2> /dev/null | md5sum | cut -d' ' -f1)
echo "file read again straight after the stop, md5: $MD5_DURING  ($([ "$MD5_DURING" = "$MD5_BEFORE" ] && echo same file || echo DIFFERENT))"

echo
echo "== 4. wait until the NameNode gives the node up and copies the blocks again"
echo "(a DataNode is declared dead after $(( ( 2 * $(hdfs getconf -confKey dfs.namenode.heartbeat.recheck-interval 2>/dev/null) + 10 * 3000 ) / 1000 )) s without a heartbeat)"
for n in $(seq 1 120); do [ "$(dead)" -ge 1 ] && break; sleep 10; done
echo "declared dead after $(( $(date +%s) - T0 )) s; live DataNodes: $(live)"
for n in $(seq 1 60); do
  replicas | grep -q "$VICTIM_PORT" || [ "$(replicas | head -1 | grep -oE '[0-9.]+:5[0-9]{4}' | wc -l)" -lt 3 ] || break
  sleep 5
done
echo "three replicas again after $(( $(date +%s) - T0 )) s ($(date +%H:%M:%S))"
replicas; summary
MD5_AFTER=$(hadoop fs -cat $F 2> /dev/null | md5sum | cut -d' ' -f1)
echo "md5 after re-replication: $MD5_AFTER  ($([ "$MD5_AFTER" = "$MD5_BEFORE" ] && echo same file || echo DIFFERENT))"

echo
echo "== 5. clean up: test file removed, extra DataNodes stopped"
hadoop fs -rm -r -f -skipTrash $DIR > /dev/null 2>&1
sleep 20
LEFT=$(hdfs fsck / -files -blocks -locations 2>/dev/null | grep -cE ':5[234]010')
echo "blocks of other files with a replica on an extra DataNode: $LEFT"
if [ "$LEFT" = 0 ]; then for i in 2 3 4; do kill "$(cat /tmp/dn$i/pid)" 2> /dev/null; done; echo "extra DataNodes stopped"; else echo "extra DataNodes left running"; fi
[ "$MD5_DURING" = "$MD5_BEFORE" ] && [ "$MD5_AFTER" = "$MD5_BEFORE" ] || { echo "ERROR: the file changed"; exit 1; }
