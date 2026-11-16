#!/bin/bash
# 02 - HDFS: mkdir, put, ls, cat.
TEST=02_hdfs; . "$(dirname "$0")/common.sh"
D=/user/bigdata/smoke_hdfs

run "hdfs dfs -rm -r -f -skipTrash $D"
run "hdfs dfs -mkdir -p $D";                               must "mkdir"
run "printf 'hello data lake\nsecond line\n' > /tmp/smoke_hello.txt"
run "hdfs dfs -put -f /tmp/smoke_hello.txt $D/";           must "put"
run "hdfs dfs -ls $D";                                     expect "ls shows the file" "$D/smoke_hello.txt"
run "hdfs dfs -cat $D/smoke_hello.txt";                    expect "cat returns the content" "^hello data lake\$"
run "hdfs dfs -ls smoke_hdfs";                             expect "relative path resolves to /user/bigdata" "smoke_hdfs/smoke_hello.txt"

echo; echo "--- clean up"
run "hdfs dfs -rm -r -skipTrash $D";                       must "clean up"
rm -f /tmp/smoke_hello.txt
finish
