#!/usr/bin/env python3
"""
Step 3 of data preparation: sample files for the large-format sources.

The company keeps call recordings (WAV), office CCTV footage and dashcam
footage. None of these files were shared (they contain voices and faces), so
this script creates SYNTHETIC stand-ins with the same technical properties:

  audio/  one WAV per order-intake call of the real extract day, branch A.
          PCM 16-bit mono 8 kHz (telephony quality), length = talk time of the
          call in the PBX log. Content is shaped noise, not speech.
  video/cctv/     one full ten-minute 1080p segment (about 150 MB, larger than
                  one HDFS block) and a few short low-resolution clips.
  video/dashcam/  short 720p clips, as pulled from a car on request.
  media_catalog/  cctv_cameras.csv, cctv_segments.csv, dashcam_requests.jsonl

Needs ffmpeg on the PATH for the video part.
Usage: python3 03_generate_media.py <real_dir> <synthetic_dir> <media_out_dir> [max_wav_files]
"""
import csv
import datetime as dt
import json
import os
import random
import subprocess
import sys
import wave

import numpy as np

R = random.Random(312)
NP = np.random.default_rng(312)
RATE = 8000


def synth_call(seconds):
    """Speech-shaped noise: low-passed white noise gated by syllable-length bursts."""
    n = int(seconds * RATE)
    noise = NP.normal(0, 1, n + 8)
    low = np.convolve(noise, np.ones(8) / 8, mode="valid")[:n]
    env = np.zeros(n)
    i = 0
    while i < n:
        on = int(R.uniform(0.08, 0.45) * RATE)
        off = int(R.uniform(0.03, 0.6) * RATE)
        env[i:i + on] = R.uniform(0.35, 1.0)
        i += on + off
    return (low * env * 9000).clip(-32000, 32000).astype("<i2")


def gen_audio(synthetic_dir, out, max_files):
    cdr = f"{synthetic_dir}/cdr/branch=A/cdr_A_20261116.csv"
    with open(cdr) as f:
        calls = [r for r in csv.DictReader(f) if r["call_type"] == "ORDER_INTAKE" and r["disposition"] == "ANSWERED" and r["order_id"]]
    calls.sort(key=lambda r: r["calldate"])
    target = f"{out}/audio/branch=A/date=2026-11-16"
    os.makedirs(target, exist_ok=True)
    for stale in os.listdir(target):
        if stale.endswith(".wav"):
            os.remove(os.path.join(target, stale))
    total = 0
    for r in calls[:max_files]:
        secs = min(120, max(4, int(r["billsec"])))
        path = f"{target}/{r['recordingfile']}"
        with wave.open(path, "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(RATE)
            w.writeframes(synth_call(secs).tobytes())
        total += os.path.getsize(path)
    print(f"audio: {min(len(calls), max_files)} wav files, {total / 1e6:.1f} MB")


def ffmpeg(args):
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y"] + args, check=True)


def gen_video(synthetic_dir, out):
    cams, cam_id = [], 1
    places = {"A": ["entrance", "reception", "call-centre hall 1", "call-centre hall 2", "dispatch room", "cash desk", "server room", "corridor 1", "corridor 2",
                    "driver lounge", "parking gate", "parking north", "parking south", "garage bay 1", "garage bay 2", "warehouse", "back door", "perimeter"],
              "B": ["entrance", "reception", "call-centre hall", "dispatch room", "cash desk", "server room", "corridor", "driver lounge",
                    "parking gate", "parking", "garage bay", "warehouse", "back door", "perimeter"]}
    for b, bid in (("A", 1), ("B", 2)):
        for p in places[b]:
            cams.append([cam_id, bid, p, "1920x1080", 15, 2000, "H.264", 30])
            cam_id += 1
    cat = f"{synthetic_dir}/media_catalog"
    os.makedirs(cat, exist_ok=True)
    with open(f"{cat}/cctv_cameras.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["camera_id", "branch_id", "location", "resolution", "fps", "bitrate_kbps", "codec", "retention_days"])
        w.writerows(cams)

    seg_rows, seg_id = [], 1
    cdir = f"{out}/video/cctv/branch=A/date=2026-11-16"
    os.makedirs(cdir, exist_ok=True)
    # one full-size ten-minute segment as an NVR writes it
    name = "cam03_20261116T080000.mp4"
    ffmpeg(["-f", "lavfi", "-i", "testsrc2=size=1920x1080:rate=15", "-t", "600", "-vf", "noise=alls=22:allf=t+u",
            "-c:v", "libx264", "-preset", "veryfast", "-b:v", "2000k", "-minrate", "2000k", "-maxrate", "2000k", "-bufsize", "4000k",
            "-pix_fmt", "yuv420p", "-an", f"{cdir}/{name}"])
    seg_rows.append([seg_id, 3, 1, "2026-11-16 08:00:00", 600, "H.264", 1920, 1080, 15, os.path.getsize(f"{cdir}/{name}"), name]); seg_id += 1
    for cam, start in ((1, "080000"), (5, "080000"), (7, "081000"), (11, "081000"), (12, "082000")):
        name = f"cam{cam:02d}_20261116T{start}_preview.mp4"
        ffmpeg(["-f", "lavfi", "-i", f"testsrc=size=640x360:rate=15", "-t", "20", "-vf", "noise=alls=15:allf=t", "-c:v", "libx264", "-preset", "veryfast",
                "-b:v", "600k", "-pix_fmt", "yuv420p", "-an", f"{cdir}/{name}"])
        seg_rows.append([seg_id, cam, 1, f"2026-11-16 {start[:2]}:{start[2:4]}:00", 20, "H.264", 640, 360, 15, os.path.getsize(f"{cdir}/{name}"), name]); seg_id += 1
    with open(f"{cat}/cctv_segments.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["segment_id", "camera_id", "branch_id", "start_ts", "duration_s", "codec", "width", "height", "fps", "size_bytes", "file_name"])
        w.writerows(seg_rows)

    # dashcam clips: footage stays on the card in the car; a clip is pulled only when somebody asks for it
    # (complaint, accident, fare dispute). The catalogue lists those requests for own-fleet trips of the last week.
    with open(f"{synthetic_dir}/reference/drivers.csv") as f:
        own = {(r["branch_id"], r["cod_drv"]): r["car_id"] for r in csv.DictReader(f) if r["employment_type"] == "OWN_FLEET"}
    requests, rid = [], 1
    days = [dt.date(2026, 11, 10) + dt.timedelta(days=i) for i in range(7)]
    sources = [(b, d, f"{synthetic_dir}/orders/branch={b}/orders_{b}_{d:%Y%m%d}.csv") for b in "AB" for d in days]
    sources = [s for s in sources if os.path.exists(s[2])]
    sources.append(("A", dt.date(2026, 11, 16), f"{os.path.dirname(synthetic_dir)}/real/orders_branchA_20261116.csv"))
    for b, d, path in sources:
        bid = "1" if b == "A" else "2"
        seen = set()
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if not r["D_VIPOL"] or not r["D_SPARKA"] or (bid, r["COD_DRV"]) not in own or r["ORDER_ID"] in seen or R.random() > 0.11:
                    continue
                seen.add(r["ORDER_ID"])
                done = dt.datetime.strptime(r["D_VIPOL"], "%Y-%m-%d %H:%M:%S")
                requests.append({"request_id": f"dc{rid:07d}", "requested_at": (done + dt.timedelta(minutes=R.randint(5, 600))).strftime("%Y-%m-%dT%H:%M:%S"),
                                 "branch": b, "car_id": int(own[(bid, r["COD_DRV"])]), "cod_drv": int(r["COD_DRV"]), "order_id": int(r["ORDER_ID"]),
                                 "trip_date": d.isoformat(),
                                 "reason": R.choices(["CLIENT_COMPLAINT", "FARE_DISPUTE", "ROAD_ACCIDENT", "LOST_ITEM", "INTERNAL_CHECK"], weights=[.4, .22, .08, .15, .15])[0],
                                 "requested_by": R.choice(["QUALITY_CONTROL", "QUALITY_CONTROL", "SECURITY", "INSURANCE"]),
                                 "clip": {"file_name": None, "duration_s": 20, "resolution": "1280x720", "fps": 30}})
                rid += 1
    ddir = f"{out}/video/dashcam/branch=A/date=2026-11-16"
    if os.path.isdir(ddir):
        for old in os.listdir(ddir):
            os.remove(os.path.join(ddir, old))
    os.makedirs(ddir, exist_ok=True)
    todays = [e for e in requests if e["branch"] == "A" and e["trip_date"] == "2026-11-16"][:4]
    for e in todays:
        name = f"{e['request_id']}_car{e['car_id']:04d}.mp4"
        ffmpeg(["-f", "lavfi", "-i", "smptehdbars=size=1280x720:rate=30", "-t", "20", "-vf", "noise=alls=30:allf=t+u", "-c:v", "libx264", "-preset", "veryfast",
                "-b:v", "3000k", "-maxrate", "3000k", "-bufsize", "6000k", "-pix_fmt", "yuv420p", "-an", f"{ddir}/{name}"])
        e["clip"]["file_name"] = name
        e["clip"]["size_bytes"] = os.path.getsize(f"{ddir}/{name}")
    for stale in ("dashcam_events.jsonl",):
        if os.path.exists(f"{cat}/{stale}"):
            os.remove(f"{cat}/{stale}")
    with open(f"{cat}/dashcam_requests.jsonl", "w") as f:
        for e in sorted(requests, key=lambda x: x["requested_at"]):
            f.write(json.dumps(e) + "\n")
    print(f"video: {len(seg_rows)} cctv files, {len(todays)} dashcam clips, {len(requests)} dashcam clip requests, {len(cams)} cameras")


def main():
    real_dir, synthetic_dir, out = sys.argv[1], sys.argv[2], sys.argv[3]
    max_files = int(sys.argv[4]) if len(sys.argv) > 4 else 240
    gen_audio(synthetic_dir, out, max_files)
    gen_video(synthetic_dir, out)


if __name__ == "__main__":
    main()
