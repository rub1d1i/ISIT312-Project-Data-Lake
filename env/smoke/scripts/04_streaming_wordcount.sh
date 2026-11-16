#!/bin/bash
# 04 - Hadoop Streaming on YARN with a python3 mapper and reducer (word count).
TEST=04_streaming_wordcount; . "$(dirname "$0")/common.sh"
D=/user/bigdata/smoke_streaming

run "cat $S/wc_mapper.py"
run "cat $S/wc_reducer.py"
run "hdfs dfs -rm -r -f -skipTrash $D"
run "hdfs dfs -mkdir -p $D/in"
run "printf 'the quick brown fox\nthe lazy dog\nThe fox\n' > /tmp/smoke_words.txt"
run "hdfs dfs -put -f /tmp/smoke_words.txt $D/in/";  must "input uploaded"

run "hadoop jar /usr/share/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar \
  -files $S/wc_mapper.py,$S/wc_reducer.py \
  -mapper 'python3 wc_mapper.py' \
  -reducer 'python3 wc_reducer.py' \
  -input $D/in -output $D/out"
must   "streaming job exit code 0"
expect "job was submitted to YARN" "Submitted application application_[0-9]+_[0-9]+"
expect "job completed successfully" "completed successfully"

run "hdfs dfs -cat $D/out/part-*"
expect "the = 3" "^the	3\$"
expect "fox = 2" "^fox	2\$"
expect "dog = 1" "^dog	1\$"

echo; echo "--- clean up"
run "hdfs dfs -rm -r -skipTrash $D"; must "clean up"
rm -f /tmp/smoke_words.txt
finish
