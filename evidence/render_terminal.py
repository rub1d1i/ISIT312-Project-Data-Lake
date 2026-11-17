#!/usr/bin/env python3
"""
Turns a captured terminal log (evidence/logs/*.log) into a PNG for the report.

The log is the real output of a command run in the lake container; this script
only draws that text as an image, like a screenshot of the terminal window.
Lines can be selected so that long logs stay readable; omitted parts are marked.

  python3 render_terminal.py logs/x.log shots/x.png [--lines A-B] [--keep REGEX] [--drop REGEX]
                                                    [--head N] [--tail N] [--max-cols N]
"""
import argparse
import re

from PIL import Image, ImageDraw, ImageFont

PROMPT = re.compile(r"^(bigdata@datalake:\S*\$ |0: jdbc:hive2://localhost:10000> |scala> |hbase\(main\):\S+ )")
FONT_PATHS = ["/System/Library/Fonts/Menlo.ttc", "/System/Library/Fonts/Monaco.ttf", "/Library/Fonts/Courier New.ttf"]


def font(size):
    for p in FONT_PATHS:
        try:
            return ImageFont.truetype(p, size)
        except OSError:
            continue
    return ImageFont.load_default()


def select(lines, a):
    if a.keep:
        k = re.compile(a.keep)
        out, skipped = [], 0
        for line in lines:
            if k.search(line):
                if skipped:
                    out.append(f"[... {skipped} lines omitted ...]")
                    skipped = 0
                out.append(line)
            else:
                skipped += 1
        if skipped:
            out.append(f"[... {skipped} lines omitted ...]")
        lines = out
    if a.drop:
        d = re.compile(a.drop)
        lines = [line for line in lines if not d.search(line)]
    if a.head and a.tail and len(lines) > a.head + a.tail:
        lines = lines[:a.head] + [f"[... {len(lines) - a.head - a.tail} lines omitted ...]"] + lines[-a.tail:]
    elif a.head and len(lines) > a.head:
        lines = lines[:a.head] + [f"[... {len(lines) - a.head} more lines ...]"]
    elif a.tail and len(lines) > a.tail:
        lines = [f"[... {len(lines) - a.tail} lines omitted ...]"] + lines[-a.tail:]
    return lines


def main():
    p = argparse.ArgumentParser()
    p.add_argument("log")
    p.add_argument("png")
    p.add_argument("--keep")
    p.add_argument("--drop")
    p.add_argument("--head", type=int)
    p.add_argument("--tail", type=int)
    p.add_argument("--lines", help="A-B[,C-D...]: use only these line ranges of the log (1-based), plus the first line (the command)")
    p.add_argument("--max-cols", type=int, default=150)
    p.add_argument("--size", type=int, default=22)
    a = p.parse_args()

    with open(a.log, encoding="utf-8", errors="replace") as f:
        lines = [line.rstrip("\n").replace("\t", "    ") for line in f]
    if a.lines:
        picked, last = lines[:1], 1
        for rng in a.lines.split(","):
            lo, hi = (int(x) for x in rng.split("-"))
            lo = max(lo, last + 1)
            if lo - last - 1 > 0:
                picked.append(f"[... {lo - last - 1} lines omitted ...]")
            picked += lines[lo - 1:hi]
            last = hi
        if last < len(lines):
            picked.append(f"[... {len(lines) - last} more lines ...]")
        lines = picked
    while lines and not lines[-1].strip():
        lines.pop()
    lines = select(lines, a)
    # a long command wraps as it would in a terminal window; long output lines are cut and marked
    shaped = []
    for line in lines:
        if PROMPT.match(line) and len(line) > a.max_cols:
            shaped.append(line[:a.max_cols])
            rest = line[a.max_cols:]
            while rest:
                shaped.append("    " + rest[:a.max_cols - 4])
                rest = rest[a.max_cols - 4:]
        elif len(line) > a.max_cols:
            shaped.append(line[:a.max_cols - 3] + "...")
        else:
            shaped.append(line)
    lines = shaped

    fnt = font(a.size)
    cw = fnt.getlength("M")
    lh = int(a.size * 1.38)
    pad = 26
    width = int(max(len(line) for line in lines) * cw) + 2 * pad
    width = max(width, 900)
    height = lh * len(lines) + 2 * pad
    img = Image.new("RGB", (width, height), "white")
    d = ImageDraw.Draw(img)
    for i, line in enumerate(lines):
        y = pad + i * lh
        prompt = PROMPT.match(line)
        if prompt:
            d.text((pad, y), prompt.group(1), font=fnt, fill=(0, 0, 0))
            d.text((pad + fnt.getlength(prompt.group(1)), y), line[len(prompt.group(1)):], font=fnt, fill=(0, 0, 0))
            # prompt lines are underlined lightly so commands stand out from output
            d.line((pad, y + lh - 5, pad + fnt.getlength(line), y + lh - 5), fill=(170, 170, 170), width=1)
        elif line.startswith("[...") and line.endswith("...]"):
            d.text((pad, y), line, font=fnt, fill=(120, 120, 120))
        else:
            d.text((pad, y), line, font=fnt, fill=(25, 25, 25))
    d.rectangle((0, 0, width - 1, height - 1), outline=(90, 90, 90), width=2)
    img.save(a.png)
    print("rendered", a.png, f"{len(lines)} lines, {width}x{height}")


if __name__ == "__main__":
    main()
