#!/usr/bin/env python3
"""
Step 1 of data preparation: turn the two Excel extracts received from the
company into flat CSV files that look like a nightly export from the branch
dispatch database (Firebird), and anonymise them on the way.

Input  (not redistributed): company_details/<orders>.xlsx, company_details/gps.xlsx
Output: data/real/orders_branchA_20261116.csv   (95 columns, original names)
        data/real/gps_branchA_20261116.csv      (12 columns, original names)
        data/real/orders_data_dictionary.csv    (column, Russian label, meaning)

Anonymisation applied here, before anything reaches the data lake:
  * coordinates are shifted by a constant offset (relative geometry is kept);
  * the single client phone number is removed;
  * the PBX host in recording URLs is replaced and recording ids, which embed
    a clock value, are replaced by opaque ids;
  * free-text order comments (may contain street names) are reduced to a
    category.
The offsets and the id salt live in a key file that the team keeps outside
the code base and does not submit.
Usage: python3 01_export_real_samples.py <orders.xlsx> <gps.xlsx> <out_dir> <key_file>
"""
import csv
import datetime
import hashlib
import json
import os
import re
import sys

import openpyxl

# English meaning of the order columns. "given" = labelled by the company in
# the extract; "inferred" = worked out by us from the values (see report).
MEANING = {
    "COD_CL": ("client id", "given"),
    "OP_PRN": ("operator / intake account that accepted the order", "given"),
    "OP_OTP": ("operator that dispatched it (1000 = automatic dispatch)", "given"),
    "COD_DRV": ("driver id", "given"),
    "COD_CAR": ("vehicle id (empty in extract)", "inferred"),
    "EADDR_FRM": ("pickup address id", "given"),
    "EADDR_TO": ("destination address id", "given"),
    "PAY": ("final price, RUB", "given"),
    "D_PZAKAZ": ("time the order was accepted", "given"),
    "D_VZAKAZ": ("requested pickup time (later than acceptance = pre-order)", "given"),
    "D_SPARKA": ("time the client was picked up", "given"),
    "D_VPRIB": ("time the driver arrived at pickup", "given"),
    "D_VIPOL": ("time the order was completed", "given"),
    "D_BUSY": ("time a driver was assigned", "given"),
    "STATUS": ("order status code", "inferred"),
    "ZAKAZ_INFO": ("free-text comment (reduced to a category here)", "given"),
    "EADDR_1": ("intermediate stop 1 (address id)", "given"),
    "BEZNAL": ("1 = non-cash corporate account", "given"),
    "WAITE_PAY": ("paid waiting amount", "given"),
    "CHANNEL": ("intake line code", "inferred"),
    "CART": ("service / tariff class", "inferred"),
    "WAVF": ("link to the call recording", "given"),
    "ORDER_ID": ("order number", "given"),
    "PODEZD": ("building entrance, typed by the operator", "given"),
    "REAZON": ("cancellation reason code", "inferred"),
    "D_VZYAL": ("time the driver took the order", "inferred"),
    "DS_STATUS": ("time of last driver status change", "inferred"),
    "NOTIFY": ("1 = client was notified automatically", "inferred"),
    "CMNT": ("intake line letter", "inferred"),
    "DISTANCE": ("route length, km", "given"),
    "D_INFORM": ("time the client was informed that the car arrived", "inferred"),
    "PODACHA": ("promised arrival time, minutes", "inferred"),
    "D_STARTWAIT": ("time waiting started", "inferred"),
    "D_DRV_PODACHA": ("time the arrival estimate was given", "inferred"),
    "D_OPAZD": ("time the car was flagged late", "inferred"),
    "ORDER_LIMIT_MASK": ("extra requirements bit mask", "given"),
    "DO_PROPOSE": ("1 = order was offered to drivers automatically", "inferred"),
    "REZERV_COD_DRV": ("reserve driver id", "inferred"),
    "MEJGOROD": ("1 = intercity order", "inferred"),
    "D_PAYSMS": ("time the payment SMS / receipt was sent", "inferred"),
    "S_CHANNEL": ("intake line code at creation", "inferred"),
    "S_PAY": ("initial quoted price, RUB", "given"),
    "AA_ORD": ("automatic assignment round", "inferred"),
    "EADDR_LAT": ("pickup latitude sent by the mobile app", "inferred"),
    "EADDR_LON": ("pickup longitude sent by the mobile app", "inferred"),
    "CARD_PAYMENT": ("1 = paid by bank card in the app", "inferred"),
    "CONTACT_PHONE": ("contact phone (removed)", "inferred"),
}


def load_key(path):
    """Coordinate offsets and id salt. The file is created once by the team and never submitted."""
    with open(path) as f:
        return json.load(f)


def opaque(uid, key):
    """Replaces a PBX recording id by an opaque 14-character id (same input, same output)."""
    return hashlib.sha1((key["id_salt"] + uid).encode()).hexdigest()[:14]


def comment_category(text):
    t = str(text).lower()
    if re.fullmatch(r"\s*\d+\s*", t):
        return "NUMERIC_CODE"
    if "звон" in t:
        return "CALLBACK_REQUEST"
    if "б/н" in t or re.search(r"\bбн\b", t) or "безнал" in t:
        return "NONCASH_NOTE"
    if "сдач" in t:
        return "CHANGE_REQUEST"
    if "багаж" in t or "терм" in t:
        return "LUGGAGE_OR_TERMINAL"
    if any(k in t for k in ("ворот", "шлагбаум", "двор", "парковк", "вход", "остановк", "подъезд")):
        return "PICKUP_POINT_HINT"
    return "LANDMARK_OR_OTHER"


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, datetime.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, float):
        return repr(round(v, 6)) if v != int(v) else str(int(v))
    return str(v).replace(",", ";").replace("\n", " ").strip()


def main():
    orders_xlsx, gps_xlsx, out_dir, key_file = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    os.makedirs(out_dir, exist_ok=True)
    key = load_key(key_file)

    # ---------------- orders ----------------
    rows = list(openpyxl.load_workbook(orders_xlsx, read_only=True, data_only=True).worksheets[0].iter_rows(values_only=True))
    labels, cols, data = rows[0], list(rows[1]), rows[2:]
    ci = {c: i for i, c in enumerate(cols)}
    out = []
    for r in data:
        r = list(r)
        if r[ci["EADDR_LAT"]] is not None:
            r[ci["EADDR_LAT"]] = round(r[ci["EADDR_LAT"]] + key["dlat"], 6)
            r[ci["EADDR_LON"]] = round(r[ci["EADDR_LON"]] + key["dlon"], 6)
        r[ci["CONTACT_PHONE"]] = None
        if r[ci["WAVF"]]:
            # the extract holds the reference in two forms (full URL, bare id); both forms are kept
            ref = re.sub(r"^https?://[^/]+", "http://pbx.branch-a.local", str(r[ci["WAVF"]]))
            uid = ref.split("uniqueid=")[-1]
            r[ci["WAVF"]] = ref[:len(ref) - len(uid)] + opaque(uid, key)
        if r[ci["ZAKAZ_INFO"]] is not None:
            r[ci["ZAKAZ_INFO"]] = comment_category(r[ci["ZAKAZ_INFO"]])
        out.append([fmt(v) for v in r])
    with open(os.path.join(out_dir, "orders_branchA_20261116.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(out)

    with open(os.path.join(out_dir, "orders_data_dictionary.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["position", "column", "label_ru", "meaning_en", "basis", "filled_rows", "total_rows"])
        for i, c in enumerate(cols):
            filled = sum(1 for r in data if r[i] is not None)
            meaning, basis = MEANING.get(c, ("not interpreted (empty or constant in the extract)" if filled == 0 or len({r[i] for r in data if r[i] is not None}) <= 1 else "not interpreted", ""))
            if c.startswith("EADDR_") and c[6:].isdigit():
                meaning, basis = (f"intermediate stop {c[6:]} (address id)", "given")
            w.writerow([i + 1, c, labels[i] or "", meaning, basis, filled, len(data)])

    # ---------------- gps ----------------
    rows = list(openpyxl.load_workbook(gps_xlsx, read_only=True, data_only=True).worksheets[0].iter_rows(values_only=True))
    gcols, gdata = list(rows[0]), rows[1:]
    gi = {c: i for i, c in enumerate(gcols)}
    with open(os.path.join(out_dir, "gps_branchA_20261116.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(gcols)
        for r in gdata:
            r = list(r)
            r[gi["LAT"]] = round(r[gi["LAT"]] + key["dlat"], 6)
            r[gi["LON"]] = round(r[gi["LON"]] + key["dlon"], 6)
            w.writerow([fmt(v) for v in r])

    print("orders rows:", len(out), "columns:", len(cols))
    print("gps rows:", len(gdata), "columns:", len(gcols))


if __name__ == "__main__":
    main()
