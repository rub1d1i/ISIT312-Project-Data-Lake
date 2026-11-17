#!/usr/bin/env python3
"""
Hadoop Streaming reducer for the engine comparison (experiment E1).

Input is sorted by the three-part key (branch, day, status). Sums the order
count and the quoted price per key.
"""
import sys

current, orders, quoted = None, 0, 0.0


def flush():
    if current is not None:
        print("%s\t%d\t%.2f" % (current, orders, quoted))


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 5:
        continue
    key = "\t".join(parts[:3])
    if key != current:
        flush()
        current, orders, quoted = key, 0, 0.0
    orders += int(parts[3])
    quoted += float(parts[4])
flush()
