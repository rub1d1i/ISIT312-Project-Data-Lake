#!/usr/bin/env python3
# Hadoop Streaming word count - mapper: one "word<TAB>1" line per word.
import sys

for line in sys.stdin:
    for word in line.strip().lower().split():
        print(f"{word}\t1")
