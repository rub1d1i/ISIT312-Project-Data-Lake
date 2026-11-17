#!/usr/bin/env python3
"""
Prints a line range "A-B" of a log for render_terminal.py --lines, found by
markers instead of fixed line numbers:

  python3 ranges.py <log> <start regex> <end regex> [nth] [end_nth]

A = line matching <start regex> (the nth match, default 1; -1 = the last match,
    -2 the one before it, and so on),
B = the end_nth line after A matching <end regex> (default 1).
"""
import re
import sys

log, start, end = sys.argv[1], re.compile(sys.argv[2]), re.compile(sys.argv[3])
nth = int(sys.argv[4]) if len(sys.argv) > 4 else 1
end_nth = int(sys.argv[5]) if len(sys.argv) > 5 else 1
lines = open(log, encoding="utf-8", errors="replace").read().split("\n")
matches = [i for i, line in enumerate(lines, 1) if start.search(line)]
a = None
if matches and 1 <= abs(nth) <= len(matches):
    a = matches[nth] if nth < 0 else matches[nth - 1]
if a is None:
    sys.exit(f"start marker not found: {sys.argv[2]}")
found = 0
for j in range(a + 1, len(lines) + 1):
    if end.search(lines[j - 1]):
        found += 1
        if found == end_nth:
            print(f"{a}-{j}")
            break
else:
    sys.exit(f"end marker not found after line {a}: {sys.argv[3]}")
