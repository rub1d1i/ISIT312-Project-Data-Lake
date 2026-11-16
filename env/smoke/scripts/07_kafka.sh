#!/bin/bash
# 07 - Kafka: topic with 3 partitions, describe, console producer (5 lines), console consumer.
TEST=07_kafka; . "$(dirname "$0")/common.sh"
B="--bootstrap-server localhost:9092"
T=smoke_events

kafka-topics.sh $B --delete --topic $T >/dev/null 2>&1 && sleep 3

run "kafka-topics.sh $B --create --topic $T --partitions 3 --replication-factor 1"
must   "topic created"
run "kafka-topics.sh $B --describe --topic $T"
expect "topic has 3 partitions" "PartitionCount: *3"
expect "replication factor 1"   "ReplicationFactor: *1"
run "kafka-topics.sh $B --list"
expect "topic is listed" "^$T\$"

run "printf 'line 1\nline 2\nline 3\nline 4\nline 5\n' | kafka-console-producer.sh $B --topic $T"
must   "5 lines produced"
run "kafka-console-consumer.sh $B --topic $T --from-beginning --max-messages 5 --timeout-ms 30000"
must   "consumer exit code 0"
for i in 1 2 3 4 5; do expect "consumed 'line $i'" "^line $i\$"; done
expect "consumer processed 5 messages" "Processed a total of 5 messages"

run "kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $T --time -1"
run "kafka-broker-api-versions.sh $B | head -1"
expect "broker advertises localhost:9092" "localhost:9092 \(id: 0"

echo; echo "--- clean up"
run "kafka-topics.sh $B --delete --topic $T"; must "topic deleted"
finish
