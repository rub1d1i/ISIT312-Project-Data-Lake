#!/usr/bin/env python3
"""
Stand-in for the position feed of the two branches.

In production a small poller in each branch reads the new rows of the
dispatch system's GPS table (rows whose ID is greater than the last one sent)
every few seconds and publishes them to Kafka. Here the recorded extract is
replayed in the same order instead.

Each row becomes one JSON message "key|value" on stdout, where the key is
<branch>:<driver id>, so that all points of one vehicle go to the same Kafka
partition and keep their order. Pipe the output into kafka-console-producer:

  python3 50_replay_gps.py --from 08:00 --to 10:30 --speed 60 --late B:6 \
      /home/bigdata/project/data/real/gps_branchA_20261116.csv:A \
      /home/bigdata/project/data/synthetic/gps/branch=B/gps_branchB_20261116.csv:B \
  | $KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic gps.positions \
      --property parse.key=true --property "key.separator=|"

--speed 60   one recorded minute is sent every second.
--late B:6   branch B's messages are held back by 6 recorded minutes, as they
             would be after a short outage of the link between branch B and
             the lake. The streaming job has to cope with this late data.
"""
import argparse
import csv
import datetime as dt
import json
import sys
import time

p = argparse.ArgumentParser()
p.add_argument("--from", dest="t_from", default="08:00")
p.add_argument("--to", dest="t_to", default="10:30")
p.add_argument("--speed", type=float, default=60.0)
p.add_argument("--late", default="", help="BRANCH:MINUTES, e.g. B:6")
p.add_argument("files", nargs="+", help="path:branch")
a = p.parse_args()
late = dict((b, int(m)) for b, m in (x.split(":") for x in a.late.split(",") if x))

rows = []
for spec in a.files:
    path, branch = spec.rsplit(":", 1)
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if a.t_from <= r["D_RECEIVE"][11:16] < a.t_to:
                event = dt.datetime.strptime(r["D_RECEIVE"], "%Y-%m-%d %H:%M:%S")
                rows.append((event + dt.timedelta(minutes=late.get(branch, 0)), int(r["ID"]), branch, r))
rows.sort(key=lambda x: (x[0], x[1]))


def num(v, cast):
    return cast(v) if v not in ("", None) else None


current, sent = None, 0
for emit, _, branch, r in rows:
    if current is not None and emit != current:
        sys.stdout.flush()
        time.sleep(60.0 / a.speed)          # the feed has one-minute resolution
    current = emit
    msg = {"id": int(r["ID"]), "branch": branch, "cod_drv": int(r["COD_DRV"]), "ts": r["D_RECEIVE"],
           "lat": float(r["LAT"]), "lon": float(r["LON"]), "speed": num(r["SPEED"], float), "track": num(r["TRACK"], float),
           "fix": int(r["FIX"]), "quality": int(r["QUALITY"]), "state": int(r["STATE_GPS_ID"]), "terminal": num(r["ID_TERMINAL"], int)}
    sys.stdout.write(f"{branch}:{r['COD_DRV']}|{json.dumps(msg)}\n")
    sent += 1
sys.stdout.flush()
sys.stderr.write(f"replayed {sent} positions from {a.t_from} to {a.t_to}" + (f", held back: {late}" if late else "") + "\n")
