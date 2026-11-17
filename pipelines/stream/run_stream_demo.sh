#!/bin/bash
# ---------------------------------------------------------------------------
# Streaming pipeline, end to end:
#   replay script -> Kafka topic gps.positions -> Spark Structured Streaming
#   -> console (fleet board), HDFS raw zone, HBase (latest position)
#
# Usage inside the lake container:
#   bash run_stream_demo.sh [from HH:MM] [to HH:MM] [speed] [seconds the job runs]
# Defaults replay everything after the cut-over of the back-fill (08:00 to the
# end of the extract, 16 Nov 2026) for both branches, one recorded minute per
# second, with branch B held back by 6 minutes; the streaming job runs on YARN
# for 240 s.
# ---------------------------------------------------------------------------
set -uo pipefail
K=$KAFKA_HOME/bin
P=/home/bigdata/project
TOPIC=gps.positions
FROM=${1:-08:00}; TO=${2:-10:30}; SPEED=${3:-60}; SECS=${4:-240}; LATE=${5:-B:6}
BS="--bootstrap-server localhost:9092"

echo "== 1. Kafka topic: 3 partitions, key = branch:driver"
if $K/kafka-topics.sh $BS --list | grep -qx "$TOPIC"; then
  $K/kafka-topics.sh $BS --delete --topic $TOPIC
  while $K/kafka-topics.sh $BS --list | grep -qx "$TOPIC"; do sleep 1; done
fi
$K/kafka-topics.sh $BS --create --topic $TOPIC --partitions 3 --replication-factor 1
$K/kafka-topics.sh $BS --describe --topic $TOPIC

echo "== 2. clean state of earlier runs"
hadoop fs -rm -r -f -skipTrash /lake/raw/stream/gps /lake/checkpoints/gps_raw_landing /lake/checkpoints/gps_latest_position > /dev/null
echo "truncate 'fleet:driver_position'" | hbase shell -n > /dev/null 2>&1 || true

echo "== 3. replay of recorded positions $FROM-$TO into Kafka (background, late: $LATE)"
( python3 $P/pipelines/stream/50_replay_gps.py --from "$FROM" --to "$TO" --speed "$SPEED" --late "$LATE" \
      $P/data/real/gps_branchA_20261116.csv:A "$P/data/synthetic/gps/branch=B/gps_branchB_20261116.csv:B" \
    | $K/kafka-console-producer.sh $BS --topic $TOPIC --property parse.key=true --property "key.separator=|" > /dev/null ) 2> /tmp/replay.log &

echo "== 4. Spark Structured Streaming job ($SECS s)"
STREAM_SECONDS=$SECS $SPARK_HOME/bin/spark-shell --master yarn --conf spark.ui.showConsoleProgress=false -i $P/pipelines/stream/51_stream_gps.scala 2> /tmp/stream_stderr.log
wait
cat /tmp/replay.log

echo "== 5. what the three sinks hold"
echo "-- messages per Kafka partition (end offsets)"
$K/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $TOPIC --time -1
echo "-- raw zone: Parquet files written by the landing query"
hadoop fs -count /lake/raw/stream/gps
hadoop fs -ls /lake/raw/stream/gps
echo "-- HBase: latest position per vehicle"
hbase shell -n <<'EOF' 2>/dev/null | grep -vE "SLF4J|^$|^nil$"
count 'fleet:driver_position'
scan 'fleet:driver_position', {LIMIT => 2, FILTER => "SingleColumnValueFilter('p', 'state', =, 'binary:2')"}
EOF
