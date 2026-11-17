#!/bin/bash
# Hadoop 2.7 stops a finished YARN container with `kill -<signal> -<pid>`. The kill from
# procps-ng misreads the negative pid and signals every process the user owns (HADOOP-12441),
# which takes down all the Hadoop daemons. The bash builtin handles it correctly.
builtin kill "$@"
