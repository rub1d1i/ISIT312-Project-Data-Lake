#!/bin/bash
# Clear pid/jps files left by daemons from a previous container run,
# otherwise hadoop-daemon.sh may think they are still running.
rm -rf /tmp/*.pid /tmp/hsperfdata_bigdata
# Week 7 (HBase): start-hbase.sh reaches its ZooKeeper over ssh to localhost.
if [ -x /usr/sbin/sshd ]; then sudo -n /usr/sbin/sshd 2>/dev/null || true; fi
exec "$@"
