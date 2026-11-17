#!/usr/bin/env python3
"""
Hadoop Streaming mapper for the engine comparison (experiment E1).

Input : raw order files (95 comma-separated columns, header line in each file).
Output: branch <TAB> day <TAB> status code   ->   1 <TAB> quoted price
The branch is not a column in the export; it is taken from the folder name
of the input file (.../branch=A/dt=2026-10-01/...).
"""
import os
import re
import sys

path = os.environ.get("mapreduce_map_input_file", os.environ.get("map_input_file", ""))
m = re.search(r"branch=([A-Z])", path)
branch = m.group(1) if m else "?"

D_PZAKAZ, STATUS, S_PAY = 10, 17, 68          # positions in the export

for line in sys.stdin:
    f = line.rstrip("\n").split(",")
    if len(f) < 69 or f[0] == "COD_CL":        # short line or header
        continue
    try:
        quoted = float(f[S_PAY] or 0)
    except ValueError:
        continue
    print("%s\t%s\t%s\t1\t%.2f" % (branch, f[D_PZAKAZ][:10], f[STATUS], quoted))
