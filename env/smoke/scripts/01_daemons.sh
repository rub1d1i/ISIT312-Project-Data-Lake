#!/bin/bash
# 01 - every daemon is running; HDFS and YARN each see their one worker.
TEST=01_daemons; . "$(dirname "$0")/common.sh"

run "jps | grep -v ' Jps\$' | sort -k2"
for d in NameNode DataNode ResourceManager NodeManager JobHistoryServer HQuorumPeer HMaster HRegionServer QuorumPeerMain Kafka; do
  expect "jps lists $d" "^[0-9]+ $d\$"
done
if [ "$(grep -c ' RunJar$' "$OUT")" -eq 2 ]; then echo "[PASS] jps lists 2 x RunJar (Hive metastore + HiveServer2)"; else echo "[FAIL] expected 2 RunJar processes"; FAILS=$((FAILS+1)); fi

run "hdfs dfsadmin -report"
expect "HDFS has 1 live datanode" "Live datanodes \(1\)"

run "hdfs dfsadmin -safemode get"
expect "HDFS is out of safe mode" "Safe mode is OFF"

run "yarn node -list"
expect "YARN has 1 running NodeManager" "Total Nodes:1"

run "lakectl status"
expect "lakectl reports every service up" "RESULT: all services are up"
finish
