#!/bin/bash
# ---------------------------------------------------------------------------
# Restart test of the streaming job (NFR2): is every position landed exactly
# once when the job stops in the middle of the stream and starts again?
#
#   1. a separate topic and separate folders, so the lake of the main run is untouched
#   2. the replay of one hour (08:00-09:00, both branches) starts in the background
#   3. the streaming job runs for 20 s and stops while the replay is still sending
#   4. the job starts again with the SAME checkpoint folders and runs until the end
#   5. landed rows = positions sent, and no position is landed twice
#
# Structured Streaming keeps the Kafka offsets it has processed in its own
# checkpoint (offsets/ and commits/), not as committed offsets of a consumer
# group; the consumer groups listed at the end are the job's temporary ones.
# Run inside the lake container:  bash run_restart_test.sh
# ---------------------------------------------------------------------------
set -uo pipefail
K=$KAFKA_HOME/bin
P=/home/bigdata/project
TOPIC=gps.restart_test
OUT=/lake/tmp/restart_test
BS="--bootstrap-server localhost:9092"
JOB="$SPARK_HOME/bin/spark-shell --master yarn --conf spark.ui.showConsoleProgress=false -i $P/pipelines/stream/51_stream_gps.scala"

echo "== 1. separate topic and folders"
if $K/kafka-topics.sh $BS --list | grep -qx "$TOPIC"; then
  $K/kafka-topics.sh $BS --delete --topic $TOPIC
  while $K/kafka-topics.sh $BS --list | grep -qx "$TOPIC"; do sleep 1; done
fi
$K/kafka-topics.sh $BS --create --topic $TOPIC --partitions 3 --replication-factor 1
hadoop fs -rm -r -f -skipTrash $OUT > /dev/null 2>&1

echo "== 2. replay 08:00-09:00 in the background (one recorded minute every two seconds)"
( python3 $P/pipelines/stream/50_replay_gps.py --from 08:00 --to 09:00 --speed 30 --late B:0 \
      $P/data/real/gps_branchA_20261116.csv:A "$P/data/synthetic/gps/branch=B/gps_branchB_20261116.csv:B" \
    | $K/kafka-console-producer.sh $BS --topic $TOPIC --property parse.key=true --property "key.separator=|" > /dev/null ) 2> /tmp/restart_replay.log &
REPLAY=$!

echo "== 3. first run of the job: 20 s of streaming, stopped while the replay is still sending"
STREAM_TOPIC=$TOPIC STREAM_LANDING=$OUT/gps STREAM_CKPT=$OUT/checkpoints STREAM_SECONDS=20 $JOB 2> /dev/null \
  | grep -E "^raw_landing|^landing folder"
echo "-- offsets recorded in the checkpoint of the landing query (last batch)"
LASTOFF=$(hadoop fs -ls $OUT/checkpoints/gps_raw_landing/offsets 2>/dev/null | awk '{print $NF}' | grep -E '/[0-9]+$' | sort -t/ -k8 -n | tail -1)
hadoop fs -cat "$LASTOFF" 2>/dev/null | tail -1; echo
wait $REPLAY
cat /tmp/restart_replay.log

echo "== 4. second run with the same checkpoints: continues from those offsets"
STREAM_TOPIC=$TOPIC STREAM_LANDING=$OUT/gps STREAM_CKPT=$OUT/checkpoints STREAM_SECONDS=60 $JOB 2> /dev/null \
  | grep -E "^raw_landing|^landing folder"

echo "== 5. positions sent to the topic (end offsets) against rows landed"
$K/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $TOPIC --time -1 \
  | awk -F: '{s += $3; print} END {print "positions in the topic: " s}'
echo "-- consumer groups (Structured Streaming does not commit offsets to them)"
$K/kafka-consumer-groups.sh $BS --list 2>/dev/null | grep -c spark-kafka-source | xargs -I{} echo "{} temporary spark-kafka-source groups"
