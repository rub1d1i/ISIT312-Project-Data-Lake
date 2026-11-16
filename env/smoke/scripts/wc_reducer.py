#!/usr/bin/env python3
# Hadoop Streaming word count - reducer: input arrives sorted by word.
import sys

current, total = None, 0
for line in sys.stdin:
    word, _, count = line.rstrip("\n").partition("\t")
    if word != current:
        if current is not None:
            print(f"{current}\t{total}")
        current, total = word, 0
    total += int(count)
if current is not None:
    print(f"{current}\t{total}")
