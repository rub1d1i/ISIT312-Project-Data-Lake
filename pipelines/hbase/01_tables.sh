#!/bin/bash
# ---------------------------------------------------------------------------
# HBase: the serving store of the lake - lookups by key in milliseconds.
# Creates the namespaces and tables if they do not exist yet (safe to re-run).
#
# fleet:driver_position - latest known position of every vehicle.
#   row key : <branch>|<driver id>          e.g. A|13010
#   family p: ts, lat, lon, speed_ms, state  (5 versions kept = short trail)
#   written by the streaming job (pipelines/stream/51_stream_gps.scala)
#
# media:recording - where the recording of a call is stored.
#   row key : <branch>|<order id>|<recording id>   all calls of an order are adjacent
#   family r: file, stored_in (packed HDFS file), offset (where the recording starts in it),
#             duration_s, size_bytes, op_id, call_ts
#   bulk loaded from the curated audio index (02_load_recordings.sh)
#
# fleet:position_history - the track of every vehicle (03_position_history.sh creates it)
#
# Run inside the lake container:  bash 01_tables.sh
# ---------------------------------------------------------------------------
set -uo pipefail
hb() { hbase shell -n 2>/dev/null | grep -vE "SLF4J|^$|^nil$"; }
for ns in fleet media; do
  # the shell pads the namespace list with spaces, so match the name as a whole word
  echo "list_namespace" | hb | grep -qE "^[[:space:]]*${ns}[[:space:]]*$" || echo "create_namespace '$ns'" | hb
done
echo "exists 'fleet:driver_position'" | hb | grep -q "does exist" || echo "create 'fleet:driver_position', {NAME => 'p', VERSIONS => 5}" | hb
echo "exists 'media:recording'" | hb | grep -q "does exist" || echo "create 'media:recording', {NAME => 'r', VERSIONS => 1}" | hb
printf "list\ndescribe 'fleet:driver_position'\n" | hb
