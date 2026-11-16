#!/bin/bash
# 03 - MapReduce on YARN: the examples jar, pi with 2 maps x 10 samples.
TEST=03_mapreduce_pi; . "$(dirname "$0")/common.sh"

run "hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar pi 2 10"
must   "job exit code 0"
expect "job was submitted to YARN (not the local runner)" "Submitted application application_[0-9]+_[0-9]+"
expect "job completed successfully" "completed successfully"
expect "pi estimate printed" "Estimated value of Pi is 3\.8"
finish
