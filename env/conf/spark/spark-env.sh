#!/usr/bin/env bash
# ISIT312 data lake: Spark environment. HADOOP_CONF_DIR is what makes spark-shell resolve
# hdfs://localhost:8020 as the default file system and find YARN for --master yarn.
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/usr/share/hadoop/etc/hadoop}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_CONF_DIR}"
