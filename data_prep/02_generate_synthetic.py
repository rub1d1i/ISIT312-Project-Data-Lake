#!/usr/bin/env python3
"""
Step 2 of data preparation: synthesise the sources the company did not share,
so that the prototype data lake has every source system to ingest.

Everything written by this script is SYNTHETIC. It is calibrated on the real
extract (data/real): same column names, same code values, same hourly demand
shape, status mix, price-per-km relation, id ranges and the same kinds of
imperfections. Real ids are reused where a table must join to the real extract.

Because rates such as "share of cancelled app orders" are copied from the real
extract into this generator, the synthetic history must not be used to draw
business conclusions: it only gives the pipelines a realistic volume and shape
to run on. Findings in the report come from the real extract alone.

Outputs (under <out_dir>):
  reference/   branches, zones, addresses, services, dictionaries, operators,
               drivers, cars, clients, corp_accounts
  orders/      branch=A|B / orders_<branch>_<yyyymmdd>.csv   (95 columns)
  payments/    branch=A|B / payments_<branch>_<yyyymmdd>.csv
  cdr/         branch=A|B / cdr_<branch>_<yyyymmdd>.csv      (PBX call log)
  sms/         sms_<yyyymmdd>.csv
  gps/         branch=B / gps_branchB_20261116.csv           (12 columns)
  app_events/  date=<yyyy-mm-dd> / events.jsonl              (nested JSON)
  chat/        chat_<yyyymmdd>.jsonl
  finance/     1c_gl_export_<yyyy-mm>.csv  (cp1251, ';', decimal comma)
  external/    competitor_prices.jsonl, traffic_index.csv

Usage: python3 02_generate_synthetic.py <real_dir> <out_dir>
"""
import bisect
import csv
import datetime as dt
import hashlib
import json
import math
import os
import random
import sys
from collections import Counter, defaultdict

SEED = 312
R = random.Random(SEED)

HIST_START = dt.date(2026, 10, 1)
HIST_END = dt.date(2026, 11, 15)          # last fully synthetic day
SAMPLE_DAY = dt.date(2026, 11, 16)        # day of the real extract (branch A)
SAMPLE_END = dt.datetime(2026, 11, 16, 10, 11)

BRANCHES = {
    "A": {"id": 1, "city": "City A", "base_orders": 2900, "role": "head office"},
    "B": {"id": 2, "city": "City B", "base_orders": 1600, "role": "regional branch"},
}
HOUR_W = [40, 32, 25, 30, 47, 51, 113, 160, 202, 160, 135, 125, 130, 135, 130, 135, 150, 185, 200, 170, 140, 115, 90, 60]
DOW_MULT = [0.95, 0.95, 0.97, 1.0, 1.2, 1.1, 0.85]
EVENT_DAYS = {dt.date(2026, 10, 24): 2.2, dt.date(2026, 11, 4): 1.6, dt.date(2026, 10, 31): 1.35}
PRICE_MULT = {7: 1.35, 8: 1.25, 17: 1.25, 18: 1.3, 19: 1.15, 0: 0.9, 1: 0.9, 2: 0.9, 3: 0.9, 4: 0.9, 5: 0.92}

SERVICES = [  # CART, name, line, base fare, per km, vehicle class
    (1, "PASSENGER_STANDARD", "PASSENGER", 239, 34.7, "SEDAN"),
    (12, "PASSENGER_MINIVAN", "PASSENGER", 265, 37.0, "MINIVAN"),
    (3, "INTERCITY_TRANSFER", "PASSENGER", 500, 100.0, "SEDAN"),
    (21, "COURIER_DELIVERY", "CARGO", 430, 38.0, "SEDAN"),
    (18, "CARGO_VAN", "CARGO", 1200, 80.0, "CARGO_VAN"),
    (11, "CARGO_TRUCK_WITH_LOADERS", "CARGO", 3500, 150.0, "TRUCK"),
    (2, "TOW_TRUCK", "SPECIAL", 3000, 90.0, "TOW"),
    (19, "TOW_TRUCK_HEAVY", "SPECIAL", 1700, 60.0, "TOW"),
]
SVC = {s[0]: s for s in SERVICES}
CART_NIGHT = [(1, .975), (12, .011), (11, .006), (18, .0025), (2, .0025), (3, .001), (19, .001), (21, .001)]
CART_DAY = [(1, .915), (12, .02), (21, .012), (18, .02), (11, .015), (3, .006), (2, .008), (19, .004)]
CART_PORTAL = [(1, .70), (12, .04), (21, .07), (18, .09), (11, .06), (3, .02), (2, .015), (19, .005)]

ORDER_COLS = None  # read from the real extract so the layout is identical


def wchoice(pairs):
    x, acc = R.random(), 0.0
    for v, p in pairs:
        acc += p
        if x <= acc:
            return v
    return pairs[-1][0]


def ts(d):
    return d.strftime("%Y-%m-%d %H:%M:%S") if d else ""


def hav_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def write_csv(path, header, rows, **kw):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding=kw.get("encoding", "utf-8")) as f:
        w = csv.writer(f, delimiter=kw.get("delimiter", ","))
        w.writerow(header)
        w.writerows(rows)


def hash_phone(client_id, branch):
    return hashlib.sha1(f"{branch}:{client_id}".encode()).hexdigest()[:12]


class WeightedPool:
    """Sampling with replacement from a fixed population with fixed weights."""

    def __init__(self, items, weights):
        self.items = items
        self.cum, acc = [], 0.0
        for w in weights:
            acc += w
            self.cum.append(acc)

    def pick(self):
        return self.items[bisect.bisect_left(self.cum, R.random() * self.cum[-1])]


# ----------------------------------------------------------------------------
# reference data
# ----------------------------------------------------------------------------
def build_reference(real_orders, real_gps, out):
    ref = {}
    lats = sorted(float(r["LAT"]) for r in real_gps)
    lons = sorted(float(r["LON"]) for r in real_gps)
    n = len(lats)
    a_box = (lats[int(n * .01)], lats[int(n * .99)], lons[int(n * .01)], lons[int(n * .99)])
    ac = ((a_box[0] + a_box[1]) / 2, (a_box[2] + a_box[3]) / 2)
    bc = (ac[0] - 1.35, ac[1] + 5.8)
    b_box = (bc[0] - 0.095, bc[0] + 0.095, bc[1] - 0.16, bc[1] + 0.16)
    boxes = {"A": a_box, "B": b_box}
    ref["boxes"] = boxes

    write_csv(f"{out}/reference/branches.csv", ["branch_id", "branch_code", "city_label", "role", "dispatch_db", "opened_year"],
              [[1, "A", "City A", "head office", "Firebird (own server room)", 2009], [2, "B", "City B", "regional branch", "Firebird (own server room)", 2016]])

    # zones: 3 rows x 4 columns grid per city
    zone_rows, zones = [], {"A": [], "B": []}
    ztype = [["SUBURB", "RESIDENTIAL", "RESIDENTIAL", "INDUSTRIAL"], ["RESIDENTIAL", "CENTRE", "CENTRE", "RESIDENTIAL"], ["INDUSTRIAL", "RESIDENTIAL", "RESIDENTIAL", "SUBURB"]]
    for b, box in boxes.items():
        dlat, dlon = (box[1] - box[0]) / 3, (box[3] - box[2]) / 4
        for i in range(3):
            for j in range(4):
                zid = BRANCHES[b]["id"] * 100 + i * 4 + j + 1
                z = (zid, BRANCHES[b]["id"], f"{b}-{'NMS'[i]}{j + 1}", ztype[i][j],
                     round(box[0] + (2 - i) * dlat, 6), round(box[0] + (3 - i) * dlat, 6), round(box[2] + j * dlon, 6), round(box[2] + (j + 1) * dlon, 6))
                zones[b].append(z)
                zone_rows.append(z)
    write_csv(f"{out}/reference/zones.csv", ["zone_id", "branch_id", "zone_code", "zone_type", "lat_min", "lat_max", "lon_min", "lon_max"], zone_rows)

    def zone_of(b, lat, lon):
        box = boxes[b]
        lat = min(max(lat, box[0]), box[1] - 1e-9)
        lon = min(max(lon, box[2]), box[3] - 1e-9)
        i = 2 - int((lat - box[0]) / ((box[1] - box[0]) / 3))
        j = int((lon - box[2]) / ((box[3] - box[2]) / 4))
        return BRANCHES[b]["id"] * 100 + i * 4 + j + 1

    def rand_point(b):
        box = boxes[b]
        c = ((box[0] + box[1]) / 2, (box[2] + box[3]) / 2)
        if R.random() < 0.7:
            lat = R.gauss(c[0], (box[1] - box[0]) / 7.5)
            lon = R.gauss(c[1], (box[3] - box[2]) / 7.0)
        else:
            lat, lon = R.uniform(box[0], box[1]), R.uniform(box[2], box[3])
        return min(max(lat, box[0]), box[1]), min(max(lon, box[2]), box[3])

    # addresses: real ids for A (app orders carry real pickup coordinates), synthetic for the rest
    addr = {"A": {}, "B": {}}
    real_from = Counter()
    for r in real_orders:
        real_from[int(r["EADDR_FRM"])] += 1
        real_from[int(r["EADDR_TO"])] += 1
        for k in range(1, 9):
            v = r[f"EADDR_{k}"]
            if v and v != "0":
                real_from[int(v)] += 0
        if r["EADDR_LAT"]:
            addr["A"][int(r["EADDR_FRM"])] = [float(r["EADDR_LAT"]), float(r["EADDR_LON"]), "RESIDENTIAL", "APP_GEO"]
    hubs = [a for a, _ in real_from.most_common(2)]
    for a in real_from:
        if a not in addr["A"]:
            lat, lon = rand_point("A")
            addr["A"][a] = [lat, lon, "RESIDENTIAL", "SYNTHETIC"]
    addr["A"][hubs[0]][2] = "HUB_RAILWAY_STATION"
    addr["A"][hubs[1]][2] = "HUB_AIRPORT"
    addr["A"][hubs[1]][0], addr["A"][hubs[1]][1] = a_box[0] + 0.01, a_box[3] - 0.01
    while len(addr["A"]) < 6000:
        a = R.randint(75100, 157700)
        if a not in addr["A"]:
            lat, lon = rand_point("A")
            addr["A"][a] = [lat, lon, wchoice([("RESIDENTIAL", .72), ("BUSINESS", .17), ("MALL", .04), ("INDUSTRIAL", .07)]), "SYNTHETIC"]
    while len(addr["B"]) < 3500:
        a = R.randint(40000, 120000)          # independent system: ids overlap with branch A
        if a not in addr["B"]:
            lat, lon = rand_point("B")
            addr["B"][a] = [lat, lon, wchoice([("RESIDENTIAL", .74), ("BUSINESS", .15), ("MALL", .04), ("INDUSTRIAL", .07)]), "SYNTHETIC"]
    bh = R.sample(sorted(addr["B"]), 2)
    addr["B"][bh[0]][2], addr["B"][bh[1]][2] = "HUB_RAILWAY_STATION", "HUB_AIRPORT"
    hub_ids = {"A": hubs, "B": bh}
    rows = []
    for b in "AB":
        for a, (lat, lon, t, src) in sorted(addr[b].items()):
            rows.append([a, BRANCHES[b]["id"], zone_of(b, lat, lon), round(lat, 6), round(lon, 6), t, src])
    write_csv(f"{out}/reference/addresses.csv", ["addr_id", "branch_id", "zone_id", "lat", "lon", "addr_type", "coord_source"], rows)
    ref["addr"], ref["zone_of"], ref["hubs"] = addr, zone_of, hub_ids
    ref["addr_pool"] = {}
    for b in "AB":
        ids = sorted(addr[b])
        w = []
        for a in ids:
            t = addr[b][a][2]
            base = R.paretovariate(1.6)
            w.append(base * (60 if t.startswith("HUB") else 6 if t == "MALL" else 2.5 if t == "BUSINESS" else 1))
        ref["addr_pool"][b] = WeightedPool(ids, w)

    write_csv(f"{out}/reference/services.csv", ["cart", "service_name", "service_line", "base_fare_rub", "per_km_rub", "vehicle_class"], SERVICES)
    write_csv(f"{out}/reference/order_status.csv", ["status", "status_name", "is_completed"],
              [[0, "CANCELLED", 0], [2, "DRIVER_AT_PICKUP_NOT_CLOSED", 0], [6, "COMPLETED", 1], [8, "CLOSED_BY_DISPATCHER", 1], [12, "COMPLETED_FARE_RECALCULATED", 1]])
    write_csv(f"{out}/reference/cancel_reasons.csv", ["reazon", "reason_name", "reason_group"],
              [[27, "CLIENT_CANCELLED", "CLIENT"], [26, "CLIENT_NO_SHOW", "CLIENT"], [28, "NO_CAR_AVAILABLE", "SUPPLY"], [3, "OPERATOR_CANCELLED", "COMPANY"],
               [14, "DRIVER_REFUSED", "SUPPLY"], [2, "CAR_BREAKDOWN", "SUPPLY"]])
    write_csv(f"{out}/reference/gps_states.csv", ["state_gps_id", "state_name", "is_on_shift"],
              [[0, "OFF_SHIFT", 0], [1, "FREE", 1], [2, "ON_ORDER", 1], [3, "INTERCITY_TRIP", 1], [6, "BREAK", 1]])
    write_csv(f"{out}/reference/intake_lines.csv", ["channel", "cmnt", "line_name"],
              [[1, "Т", "MAIN_PHONE_LINE"], [14, "А", "SECOND_PHONE_LINE"], [13, "Л", "WEB_CABINET"]])

    # operators: technical accounts identical in both branches (same software), people differ
    real_ops = sorted({int(r["OP_PRN"]) for r in real_orders} - {1583, 218})
    night_real = {2741, 2275, 2959}
    ops = {"A": [], "B": []}
    rows = [[1583, 0, "MOBILE_APP_INTAKE", "SYSTEM", ""], [218, 0, "CORPORATE_PORTAL_INTAKE", "SYSTEM", ""], [1000, 0, "AUTO_DISPATCH", "SYSTEM", ""]]
    for o in real_ops:
        sh = "NIGHT" if o in night_real else "MORNING"
        ops["A"].append((o, sh))
    used = set(real_ops) | {1583, 218, 1000}
    for sh, k in (("NIGHT", 4), ("MORNING", 4), ("DAY", 10), ("EVENING", 9)):
        for _ in range(k):
            o = R.randint(1500, 3100)
            while o in used:
                o = R.randint(1500, 3100)
            used.add(o)
            ops["A"].append((o, sh))
    usedb = set()
    for sh, k in (("NIGHT", 3), ("MORNING", 6), ("DAY", 6), ("EVENING", 5)):
        for _ in range(k):
            o = R.randint(300, 990)
            while o in usedb:
                o = R.randint(300, 990)
            usedb.add(o)
            ops["B"].append((o, sh))
    for b in "AB":
        for o, sh in ops[b]:
            rows.append([o, BRANCHES[b]["id"], "CALL_CENTRE_OPERATOR", "HUMAN", sh])
    write_csv(f"{out}/reference/operators.csv", ["op_id", "branch_id", "role", "kind", "shift"], rows)
    ref["ops"] = ops

    # drivers and own-fleet cars
    real_drv = sorted({int(r["COD_DRV"]) for r in real_gps})
    term_drv = {int(r["COD_DRV"]) for r in real_gps if r["ID_TERMINAL"]}
    cart_by_drv = defaultdict(set)
    for r in real_orders:
        if r["COD_DRV"]:
            cart_by_drv[int(r["COD_DRV"])].add(int(r["CART"]))
    drivers = {"A": {}, "B": {}}

    def vclass_from_carts(cs):
        if cs & {11}:
            return "TRUCK"
        if cs & {18}:
            return "CARGO_VAN"
        if cs & {2, 19}:
            return "TOW"
        if cs & {12}:
            return "MINIVAN"
        return None

    for d in real_drv:
        vc = vclass_from_carts(cart_by_drv.get(d, set())) or wchoice([("SEDAN", .9), ("MINIVAN", .05), ("CARGO_VAN", .04), ("TRUCK", .007), ("TOW", .003)])
        drivers["A"][d] = {"vclass": vc, "own": d in term_drv}
    while len(drivers["A"]) < 1600:
        lo, hi = wchoice([((5000, 30000), .45), ((60000, 90000), .07), ((100000, 112000), .48)])
        d = R.randint(lo, hi)
        if d not in drivers["A"]:
            drivers["A"][d] = {"vclass": wchoice([("SEDAN", .88), ("MINIVAN", .05), ("CARGO_VAN", .05), ("TRUCK", .015), ("TOW", .005)]), "own": False}
    while len(drivers["B"]) < 900:
        d = R.randint(3000, 48000)            # overlaps with branch A ids on purpose
        if d not in drivers["B"]:
            drivers["B"][d] = {"vclass": wchoice([("SEDAN", .88), ("MINIVAN", .05), ("CARGO_VAN", .05), ("TRUCK", .015), ("TOW", .005)]), "own": False}
    car_rows, drv_rows, car_id = [], [], 1
    for b, n_own in (("A", 150), ("B", 90)):
        pool = [d for d in sorted(drivers[b]) if not drivers[b][d]["own"]]
        # own fleet keeps most cargo and tow vehicles
        special = [d for d in pool if drivers[b][d]["vclass"] in ("CARGO_VAN", "TRUCK", "TOW")]
        chosen = set(R.sample(special, min(len(special), int(n_own * 0.3))))
        rest = [d for d in pool if d not in chosen]
        chosen |= set(R.sample(rest, n_own - len(chosen) - sum(1 for d in drivers[b] if drivers[b][d]["own"])))
        for d in chosen:
            drivers[b][d]["own"] = True
        for d in sorted(drivers[b]):
            info = drivers[b][d]
            info["weight"] = R.lognormvariate(0, 0.9)
            info["commission"] = 0.0 if info["own"] else R.choice([0.18, 0.18, 0.2, 0.2, 0.22])
            hire = dt.date(2012, 1, 1) + dt.timedelta(days=R.randint(0, 5300))
            cid = ""
            if info["own"]:
                cid = car_id
                car_id += 1
                car_rows.append([cid, BRANCHES[b]["id"], info["vclass"], R.randint(2017, 2026), 1052398000 + cid, f"DC-{b}-{cid:04d}", f"{b}-{cid:04d}"])
            info["car_id"] = cid
            drv_rows.append([d, BRANCHES[b]["id"], "OWN_FLEET" if info["own"] else "CONTRACTOR", cid, info["vclass"], hire.isoformat(), info["commission"], 1])
    write_csv(f"{out}/reference/drivers.csv", ["cod_drv", "branch_id", "employment_type", "car_id", "vehicle_class", "hire_date", "commission_rate", "is_active"], drv_rows)
    write_csv(f"{out}/reference/cars.csv", ["car_id", "branch_id", "vehicle_class", "model_year", "terminal_id", "dashcam_id", "fleet_no"], car_rows)
    ref["drivers"] = drivers
    ref["drv_pool"] = {}
    for b in "AB":
        by_class = defaultdict(lambda: ([], []))
        for d, info in drivers[b].items():
            by_class[info["vclass"]][0].append(d)
            by_class[info["vclass"]][1].append(info["weight"])
        ref["drv_pool"][b] = {k: WeightedPool(v[0], v[1]) for k, v in by_class.items()}

    # corporate accounts and clients
    corp_rows, corp = [], {"A": [], "B": []}
    cid = 5001
    industries = ["RETAIL", "MANUFACTURING", "IT_SERVICES", "BANKING", "HEALTHCARE", "CONSTRUCTION", "WHOLESALE", "HOSPITALITY", "PUBLIC_SECTOR", "LOGISTICS"]
    for b, k in (("A", 60), ("B", 30)):
        for _ in range(k):
            corp[b].append(cid)
            corp_rows.append([cid, BRANCHES[b]["id"], R.choice(industries), (dt.date(2015, 1, 1) + dt.timedelta(days=R.randint(0, 4200))).isoformat(), 0.22, R.choice([15, 30, 30, 45]), R.choice([50000, 100000, 250000, 500000])])
            cid += 1
    write_csv(f"{out}/reference/corp_accounts.csv", ["corp_account_id", "branch_id", "industry", "contract_start", "vat_rate", "payment_terms_days", "monthly_limit_rub"], corp_rows)
    clients = {"A": {}, "B": {}}
    for r in real_orders:
        c = int(r["COD_CL"])
        is_corp = r["BEZNAL"] == "1"
        cur = clients["A"].get(c, {"corp": None, "app": False})
        if is_corp and cur["corp"] is None:
            cur["corp"] = R.choice(corp["A"])
        if r["OP_PRN"] == "1583":
            cur["app"] = True
        clients["A"][c] = cur
    for b, target, lo, hi in (("A", 95000, 5827000, 9725000), ("B", 52000, 3000000, 6500000)):
        while len(clients[b]) < target:
            c = int(lo + (hi - lo) * (R.random() ** 0.6))
            if c not in clients[b]:
                clients[b][c] = {"corp": R.choice(corp[b]) if R.random() < 0.09 else None, "app": R.random() < 0.34}
    rows = []
    ref["cl_pool"] = {}
    for b in "AB":
        ids = sorted(clients[b])
        weights = [R.paretovariate(1.3) for _ in ids]
        pools = {"all": ([], []), "corp": ([], []), "app": ([], []), "priv": ([], [])}
        for c, w in zip(ids, weights):
            info = clients[b][c]
            pools["all"][0].append(c); pools["all"][1].append(w)
            if info["corp"]:
                pools["corp"][0].append(c); pools["corp"][1].append(w)
            else:
                pools["priv"][0].append(c); pools["priv"][1].append(w)
            if info["app"] and not info["corp"]:
                pools["app"][0].append(c); pools["app"][1].append(w)
            rows.append([c, BRANCHES[b]["id"], "CORPORATE" if info["corp"] else "PRIVATE", info["corp"] or "", 1 if info["app"] else 0,
                         (dt.date(2014, 1, 1) + dt.timedelta(days=int(4600 * ((c % 100000) / 100000.0)))).isoformat()])
        ref["cl_pool"][b] = {k: WeightedPool(v[0], v[1]) for k, v in pools.items()}
    write_csv(f"{out}/reference/clients.csv", ["cod_cl", "branch_id", "client_type", "corp_account_id", "app_user", "first_seen_date"], rows)
    ref["clients"] = clients
    return ref


# ----------------------------------------------------------------------------
# orders
# ----------------------------------------------------------------------------
def shift_ops(ops, hour):
    def on(sh):
        return {"NIGHT": hour >= 22 or hour < 7, "MORNING": 6 <= hour < 14, "DAY": 10 <= hour < 18, "EVENING": 14 <= hour < 23}[sh]
    cur = [o for o, sh in ops if on(sh)]
    return cur or [o for o, _ in ops]


def gen_order(b, t_accept, ref, seq, event_mult):
    """Returns (row dict keyed by the real column names, extra info for the other sources)."""
    h, wd = t_accept.hour, t_accept.weekday()
    business = wd < 5 and 8 <= h < 19
    p_app = 0.30 if h < 6 else 0.19
    p_web = 0.01 if h < 6 else 0.05
    p_portal = 0.17 if business else 0.03
    x = R.random()
    channel = "APP" if x < p_app else "WEB" if x < p_app + p_web else "PORTAL" if x < p_app + p_web + p_portal else "VOICE"

    row = {c: "" for c in ORDER_COLS}
    cart = wchoice(CART_PORTAL if channel == "PORTAL" else CART_DAY if 8 <= h < 19 else CART_NIGHT)
    _, _, line, base, per_km, vclass = SVC[cart]
    # non-cash (corporate invoice) is a property of the client, not of the channel: in the real extract
    # 36 of 49 non-cash orders came through operators and 11 of the 17 orders of account 218 were non-cash
    beznal = 1 if (channel == "PORTAL" and R.random() < 0.65) or (channel == "VOICE" and R.random() < (0.10 if business else 0.03)) else 0
    pool = ref["cl_pool"][b]
    client = pool["corp"].pick() if beznal else pool["app"].pick() if channel in ("APP", "WEB") else pool["all"].pick()

    a_from = ref["addr_pool"][b].pick()
    a_to = ref["addr_pool"][b].pick()
    while a_to == a_from:
        a_to = ref["addr_pool"][b].pick()
    f, t = ref["addr"][b][a_from], ref["addr"][b][a_to]
    if cart == 3:
        km = round(R.uniform(80, 350), 3)
    else:
        km = round(max(0.8, hav_km(f[0], f[1], t[0], t[1]) * 1.32 * R.gauss(1, 0.08)), 3)
    if R.random() < 0.0035:
        km = 0
    s_pay = (base + per_km * km) * PRICE_MULT.get(h, 1.0) * R.gauss(1, 0.08) * (1.2 if event_mult > 1.5 else 1.0)
    s_pay = max(100, int(round(s_pay / 10.0) * 10) if channel == "VOICE" else int(round(s_pay)))
    pay = s_pay
    if R.random() < 0.03:
        pay = s_pay + R.choice([20, 30, 50, 80, 100, 150])
    if beznal and R.random() < 0.63:
        # as in the real extract (31 of 49 non-cash orders): a round base price plus 22 %, kopecks kept in PAY
        pay = round(int(round(s_pay / 10.0) * 10) * 1.22, 2)
        s_pay = int(round(pay))

    p_cancel = {"APP": 0.40, "WEB": 0.30, "VOICE": 0.155, "PORTAL": 0.08}[channel]
    if R.random() < p_cancel:
        status = 0
    else:
        y = R.random()
        p8 = 0.16 if channel == "VOICE" else 0.05
        status = 8 if y < p8 else 2 if y < p8 + 0.012 else 12 if y < p8 + 0.016 else 6
    if status == 12 and cart != 3:
        km = round(km * 3, 3)

    preorder = R.random() < 0.047
    t_req = t_accept + dt.timedelta(minutes=R.choice([40, 60, 90, 120, 180, 300, 600, 1440])) if preorder else t_accept
    podacha = wchoice([(15, .51), (30, .33), (10, .078), (0, .047), (20, .012), (25, .012), (26, .008), (34, .003)])
    d_assign = wchoice([(0, .35), (1, .30), (2, .12), (R.randint(3, 8), .15), (R.randint(9, 25), .07), (R.randint(30, 120), .01)])
    if R.random() < 0.002:
        d_assign = -R.randint(1, 2)             # clock skew between dispatcher PC and server, as in the real extract
    t_busy = (t_req if preorder else t_accept) + dt.timedelta(minutes=d_assign)
    to_client = min(40, max(1, int(round(R.lognormvariate(math.log(7), 0.45)))))
    t_arr = t_busy + dt.timedelta(minutes=to_client)
    wait = min(30, int(R.expovariate(1 / 2.2)))
    t_pick = t_arr + dt.timedelta(minutes=wait)
    speed = min(45, max(12, R.gauss(24, 5))) * (0.8 if h in (7, 8, 17, 18) else 1.0)
    t_done = t_pick + dt.timedelta(minutes=max(1, int(round(km / speed * 60))))

    driver = ref["drv_pool"][b].get(vclass, ref["drv_pool"][b]["SEDAN"]).pick()
    reazon, has_driver, arrived, completed = "", True, True, True
    if status == 0:
        reazon = wchoice([(27, .943), (3, .026), (28, .021), ("", .01)])
        has_driver = reazon != 28 and R.random() < 0.30
        arrived = completed = False
    elif status == 8:
        if R.random() >= 0.76:
            completed = False
            reazon = wchoice([(26, .81), (27, .10), (14, .05), (2, .04)])
    elif status == 2:
        completed = False
        reazon = wchoice([(26, .72), (27, .14), ("", .14)])

    late = arrived and (t_arr - t_req).total_seconds() / 60 > max(podacha, 10)
    notify = 1 if R.random() < 0.69 else 0
    card = 1 if channel == "APP" and R.random() < 0.30 else 0

    row.update({
        "COD_CL": client, "OP_PRN": {"APP": 1583, "WEB": 1583, "PORTAL": 218}.get(channel) or R.choice(shift_ops(ref["ops"][b], h)),
        "OP_OTP": 1000 if R.random() < 0.95 else R.choice(shift_ops(ref["ops"][b], h)),
        "COD_DRV": driver if has_driver else "", "EADDR_FRM": a_from, "EADDR_TO": a_to, "PAY": pay,
        "D_PZAKAZ": ts(t_accept), "D_VZAKAZ": ts(t_req), "D_BUSY": ts(t_busy) if has_driver else "",
        "D_VPRIB": ts(t_arr) if arrived else "", "D_SPARKA": ts(t_pick) if completed else "", "D_VIPOL": ts(t_done) if completed else "",
        "STATUS": status, "BEZNAL": beznal, "WAITE_PAY": 0, "CART": cart, "ORDER_ID": seq["order"], "REAZON": reazon, "CNT_RETR": 0,
        "D_VZYAL": ts(t_busy) if has_driver and R.random() < 0.96 else "",
        "DS_STATUS": ts(t_pick if completed else t_busy if arrived else t_accept), "NOTIFY": notify, "DISTANCE": km,
        "D_INFORM": ts(t_arr) if arrived and notify and R.random() < 0.75 else "", "PODACHA": podacha,
        "D_STARTWAIT": ts(t_arr) if arrived else "", "CNT_CL_ACK": 0, "D_DRV_PODACHA": ts(t_accept),
        "D_OPAZD": ts(t_req + dt.timedelta(minutes=max(podacha, 10))) if late or (status == 0 and R.random() < 0.18) else "",
        "ORDER_LIMIT_MASK": wchoice([(0, .947), (2048, .042), (1, .009), (2, .002)]), "DO_PROPOSE": 1 if R.random() < 0.82 else 0,
        "DRV_NEAR": 0, "BONUS": 0, "MEJGOROD": 1 if cart == 3 else 0, "POCHAS": 0, "S_PAY": s_pay, "ZAKAZ_NARYAD": 0,
        "IS_TAXOMETR": 0, "FREE_BONUS": 0 if R.random() < 0.96 else "", "SHOW_IN_HISTORY": 1, "CARD_PAYMENT": card,
        "REZERV_COD_DRV": ref["drv_pool"][b]["SEDAN"].pick() if R.random() < 0.027 else "",
        "AA_ORD": wchoice([(1, .8), (2, .17), (3, .025), (4, .005)]) if R.random() < 0.44 else "",
    })
    if channel == "VOICE":
        second = R.random() < 0.025
        row.update({"CHANNEL": 14 if second else 1, "S_CHANNEL": 14 if second else 1, "CMNT": "А" if second else "Т", "NETOCH_ADDR_ID": 0})
        if R.random() < 0.997:
            seq["call"] += 1
            uid = hashlib.sha1(f"{b}:{seq['call']}".encode()).hexdigest()[:14]          # opaque recording id
            # the real extract stores the reference either as a PBX URL or as the bare id (451 against 138 rows)
            row["WAVF"] = uid if R.random() < 0.23 else f"http://pbx.branch-{b.lower()}.local/cdr/recording.php?uniqueid={uid}"
    elif channel == "WEB":
        row.update({"CHANNEL": 13, "S_CHANNEL": 13, "CMNT": "Л"})
    else:
        row.update({"CHANNEL": 1, "S_CHANNEL": 1, "CMNT": "Т"})
    if channel == "APP" and R.random() < 0.73:
        row.update({"EADDR_COORDS": 1, "EADDR_LAT": round(f[0] + R.gauss(0, 0.0004), 6), "EADDR_LON": round(f[1] + R.gauss(0, 0.0007), 6)})
    if completed and (card or (channel == "APP" and R.random() < 0.25)):
        row["D_PAYSMS"] = ts(t_done)
    if R.random() < 0.48:
        row["PODEZD"] = R.choice(["ч д", "чд", "ч/д", "один", "1п", "п1", "п3", "п4", "ворота", "азс"]) if R.random() < 0.22 else R.choice([1, 1, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8])
    if R.random() < 0.41:
        row["ZAKAZ_INFO"] = wchoice([("PICKUP_POINT_HINT", .30), ("LANDMARK_OR_OTHER", .45), ("CALLBACK_REQUEST", .10), ("NONCASH_NOTE", .06),
                                     ("CHANGE_REQUEST", .04), ("LUGGAGE_OR_TERMINAL", .03), ("NUMERIC_CODE", .02)])
    if R.random() < 0.026:
        row["EADDR_1"] = ref["addr_pool"][b].pick()

    extra = {"branch": b, "channel": channel, "client": client, "driver": driver if has_driver else None, "t_accept": t_accept, "t_busy": t_busy if has_driver else None,
             "t_arr": t_arr if arrived else None, "t_pick": t_pick if completed else None, "t_done": t_done if completed else None, "status": status,
             "pay": pay, "beznal": beznal, "card": card, "cart": cart, "line": line, "order_id": seq["order"], "wavf": row["WAVF"], "notify": notify,
             "op": row["OP_PRN"], "d_inform": row["D_INFORM"], "d_paysms": row["D_PAYSMS"], "from": a_from, "to": a_to, "km": km, "s_pay": s_pay}
    seq["order"] += 1 + (1 if R.random() < 0.13 else 0) + (R.randint(2, 6) if R.random() < 0.004 else 0)
    return row, extra


def real_extra(r):
    """Same 'extra' structure for a row of the real extract, so derived sources cover the real day too."""
    def p(v):
        return dt.datetime.strptime(v, "%Y-%m-%d %H:%M:%S") if v else None
    ch = "PORTAL" if r["OP_PRN"] == "218" else ("WEB" if r["CMNT"] == "Л" else "APP") if r["OP_PRN"] == "1583" else "VOICE"
    done = p(r["D_VIPOL"]) if r["D_SPARKA"] else None
    return {"branch": "A", "channel": ch, "client": int(r["COD_CL"]), "driver": int(r["COD_DRV"]) if r["COD_DRV"] else None, "t_accept": p(r["D_PZAKAZ"]),
            "t_busy": p(r["D_BUSY"]), "t_arr": p(r["D_VPRIB"]), "t_pick": p(r["D_SPARKA"]), "t_done": done, "status": int(r["STATUS"]),
            "pay": float(r["PAY"]), "beznal": int(r["BEZNAL"]), "card": int(r["CARD_PAYMENT"]), "cart": int(r["CART"]), "line": SVC[int(r["CART"])][2],
            "order_id": int(r["ORDER_ID"]), "wavf": r["WAVF"], "notify": int(r["NOTIFY"]), "op": int(r["OP_PRN"]), "d_inform": r["D_INFORM"],
            "d_paysms": r["D_PAYSMS"], "from": int(r["EADDR_FRM"]), "to": int(r["EADDR_TO"]), "km": float(r["DISTANCE"]), "s_pay": float(r["S_PAY"])}


def gen_orders(ref, real_orders, out):
    days = [HIST_START + dt.timedelta(days=i) for i in range((HIST_END - HIST_START).days + 1)]
    # branch A history must end just below the first real order number
    seq = {"A": {"order": 0, "call": 23000000}, "B": {"order": 41207300, "call": 8100000}}
    plan = {}
    for b in "AB":
        for d in days + ([SAMPLE_DAY] if b == "B" else []):
            n = int(BRANCHES[b]["base_orders"] * DOW_MULT[d.weekday()] * EVENT_DAYS.get(d, 1.0) * R.gauss(1, 0.05))
            plan[(b, d)] = n
    total_a = sum(n for (b, d), n in plan.items() if b == "A")
    seq["A"]["order"] = min(int(r["ORDER_ID"]) for r in real_orders) - int(total_a * 1.17) - 500
    extras, stats = defaultdict(list), Counter()
    for b in "AB":
        for d in sorted(x for (bb, x) in plan if bb == b):
            n = plan[(b, d)]
            times = []
            for _ in range(n):
                h = R.choices(range(24), weights=HOUR_W)[0]
                times.append(dt.datetime(d.year, d.month, d.day, h, R.randint(0, 59)))
            if d == SAMPLE_DAY:
                times = [t for t in times if t <= SAMPLE_END]
            times.sort()
            rows = []
            for t in times:
                row, ex = gen_order(b, t, ref, seq[b], EVENT_DAYS.get(d, 1.0))
                rows.append([row[c] for c in ORDER_COLS])
                extras[(b, d)].append(ex)
                stats[(b, ex["channel"])] += 1
                stats[("status", ex["status"])] += 1
            # INJECTED DEFECT (not seen in the real extract): rows re-sent by the branch exchange, i.e. exact
            # duplicates. They are added on purpose so that the de-duplication step of the pipeline is tested.
            for _ in range(int(len(rows) * 0.003)):
                rows.insert(R.randint(0, len(rows)), list(R.choice(rows)))
            write_csv(f"{out}/orders/branch={b}/orders_{b}_{d:%Y%m%d}.csv", ORDER_COLS, rows)
            stats[("orders", b)] += len(times)
            stats[("order_files", b)] += 1
    for r in real_orders:
        extras[("A", SAMPLE_DAY)].append(real_extra(r))
    return extras, stats, seq


# ----------------------------------------------------------------------------
# sources derived from orders
# ----------------------------------------------------------------------------
def gen_payments(extras, ref, out, stats):
    pid = 900000001
    for (b, d), lst in sorted(extras.items()):
        rows = []
        for e in lst:
            if not e["t_done"]:
                continue
            rate = ref["drivers"][b].get(e["driver"], {"commission": 0.2})["commission"]
            method = "CORP_INVOICE" if e["beznal"] else "CARD" if e["card"] else "CASH"
            commission = round(e["pay"] * rate, 2)
            rows.append([pid, BRANCHES[b]["id"], e["order_id"], ts(e["t_done"] + dt.timedelta(minutes=R.randint(0, 3))), e["pay"], "RUB", method,
                         e["driver"], commission, round(e["pay"] - commission, 2) if rate else 0, "PENDING_INVOICE" if method == "CORP_INVOICE" else "SETTLED"])
            pid += 1
        write_csv(f"{out}/payments/branch={b}/payments_{b}_{d:%Y%m%d}.csv",
                  ["payment_id", "branch_id", "order_id", "paid_at", "amount", "currency", "method", "cod_drv", "commission_amount", "driver_payout", "state"], rows)
        stats[("payments", b)] += len(rows)


def gen_cdr(extras, ref, out, stats, seq):
    """PBX call detail records. ~3 calls per order: the intake call plus inquiries, driver-client bridge calls and auto-informer calls."""
    for (b, d), lst in sorted(extras.items()):
        rows = []
        for e in lst:
            def add(t, kind, order, uid=None, answered_p=0.9, mean_talk=20):
                if uid is None:
                    seq[b]["call"] += 1
                    uid = hashlib.sha1(f"{b}:{seq[b]['call']}".encode()).hexdigest()[:14]
                x = R.random()
                disp = "ANSWERED" if x < answered_p else "NO ANSWER" if x < answered_p + 0.07 else "BUSY" if x < answered_p + 0.09 else "FAILED"
                wait_q = int(R.expovariate(1 / 9.0)) if kind in ("ORDER_INTAKE", "STATUS_INQUIRY", "COMPLAINT") else 0
                talk = max(3, int(R.lognormvariate(math.log(mean_talk), 0.6))) if disp == "ANSWERED" else 0
                op = e["op"] if kind == "ORDER_INTAKE" else R.choice(shift_ops(ref["ops"][b], t.hour)) if kind in ("STATUS_INQUIRY", "COMPLAINT") else ""
                direction = "OUT" if kind in ("AUTO_INFORMER", "OPERATOR_CALLBACK") else "BRIDGE" if kind == "DRIVER_CLIENT_BRIDGE" else "IN"
                rows.append([ts(t), uid, BRANCHES[b]["id"], hash_phone(e["client"], b), "200" if kind == "ORDER_INTAKE" and R.random() < 0.025 else "100",
                             direction, kind, op, wait_q + talk + R.randint(2, 6), talk, disp, wait_q, f"{uid}.wav" if disp == "ANSWERED" else "", order])
            if e["wavf"]:
                # the real extract stores the recording reference in two forms:
                # a full PBX URL (older rows) or the bare unique id (latest rows)
                uid = e["wavf"].split("uniqueid=")[-1]
                add(e["t_accept"] - dt.timedelta(minutes=1), "ORDER_INTAKE", e["order_id"], uid=uid, answered_p=1.0, mean_talk=40)
            if e["t_busy"] and R.random() < 0.55:
                add(e["t_busy"] + dt.timedelta(minutes=R.randint(1, 6)), "DRIVER_CLIENT_BRIDGE", e["order_id"], answered_p=0.85, mean_talk=14)
            if e["t_arr"] and e["notify"]:
                add(e["t_arr"], "AUTO_INFORMER", e["order_id"], answered_p=0.8, mean_talk=8)
            if R.random() < 0.45:
                add(e["t_accept"] + dt.timedelta(minutes=R.randint(3, 20)), "STATUS_INQUIRY", e["order_id"] if R.random() < 0.8 else "", answered_p=0.88, mean_talk=26)
            if R.random() < 0.85:
                add(e["t_accept"] + dt.timedelta(minutes=R.randint(-30, 30)), "STATUS_INQUIRY" if R.random() < 0.7 else "OPERATOR_CALLBACK", "", answered_p=0.86, mean_talk=31)
            if R.random() < 0.012:
                add(e["t_accept"] + dt.timedelta(minutes=R.randint(30, 240)), "COMPLAINT", e["order_id"], answered_p=0.95, mean_talk=95)
        rows.sort(key=lambda r: r[0])
        write_csv(f"{out}/cdr/branch={b}/cdr_{b}_{d:%Y%m%d}.csv",
                  ["calldate", "uniqueid", "branch_id", "src_hash", "dst", "direction", "call_type", "op_id", "duration", "billsec", "disposition", "queue_wait", "recordingfile", "order_id"], rows)
        stats[("calls", b)] += len(rows)
        stats[("call_seconds", b)] += sum(r[9] for r in rows)


def gen_sms(extras, out, stats):
    sid = 70000001
    by_day = defaultdict(list)
    for (b, d), lst in extras.items():
        for e in lst:
            def add(t, template):
                nonlocal sid
                x = R.random()
                by_day[d].append([sid, ts(t), BRANCHES[b]["id"], e["client"], hash_phone(e["client"], b), template, e["order_id"],
                                  "DELIVERED" if x < 0.965 else "FAILED" if x < 0.985 else "EXPIRED", 1, R.choice([2.8, 3.1, 3.5])])
                sid += 1
            if e["t_busy"] and e["notify"] and e["channel"] != "APP":
                add(e["t_busy"], "CAR_ASSIGNED")
            if e["d_inform"]:
                add(dt.datetime.strptime(e["d_inform"], "%Y-%m-%d %H:%M:%S"), "CAR_ARRIVED")
            if e["d_paysms"]:
                add(dt.datetime.strptime(e["d_paysms"], "%Y-%m-%d %H:%M:%S"), "PAYMENT_RECEIPT")
    for d, rows in sorted(by_day.items()):
        rows.sort(key=lambda r: r[1])
        write_csv(f"{out}/sms/sms_{d:%Y%m%d}.csv", ["sms_id", "sent_at", "branch_id", "cod_cl", "phone_hash", "template", "order_id", "delivery_status", "segments", "cost_rub"], rows)
        stats[("sms", "all")] += len(rows)


def gen_app_events(extras, ref, out, stats):
    """Mobile app / website clickstream as nested JSON lines, last three days only."""
    days = [dt.date(2026, 11, 14), dt.date(2026, 11, 15), SAMPLE_DAY]
    models = {"android": ["SM-A546", "Redmi Note 13", "Pixel 8", "POCO X6", "SM-S921"], "ios": ["iPhone13,2", "iPhone14,5", "iPhone15,4", "iPhone16,1"], "web": ["Chrome", "Safari", "Firefox", "YaBrowser"]}
    eid = 1
    for d in days:
        lines = []
        for b in "AB":
            sessions = []
            for e in extras.get((b, d), []):
                if e["channel"] in ("APP", "WEB"):
                    sessions.append((e, True))
                    for _ in range(R.choice([1, 2, 2, 3, 3, 4])):      # sessions that only asked for a price
                        sessions.append((e, False))
            for e, converted in sessions:
                platform = "web" if e["channel"] == "WEB" else R.choices(["android", "ios"], weights=[.67, .33])[0]
                client = e["client"] if converted else ref["cl_pool"][b]["app"].pick()
                t = e["t_accept"] - dt.timedelta(seconds=R.randint(60, 400)) if converted else e["t_accept"] + dt.timedelta(minutes=R.randint(-50, 50))
                sess = hashlib.md5(f"{b}{client}{t}{R.random()}".encode()).hexdigest()[:16]
                f = ref["addr"][b][e["from"]]
                base = {"session_id": sess, "client_id": client, "branch": b, "platform": platform,
                        "app_version": "web" if platform == "web" else R.choice(["5.8.1", "5.9.0", "5.9.2", "6.0.0"]),
                        "device": {"model": R.choice(models[platform]), "os_version": R.choice(["14", "15", "16", "17", "18"])},
                        "geo": {"lat": round(f[0] + R.gauss(0, 0.002), 6), "lon": round(f[1] + R.gauss(0, 0.003), 6), "accuracy_m": R.choice([5, 8, 12, 20, 35, 65])}}
                quote = e["s_pay"] if converted else int(e["s_pay"] * R.uniform(0.9, 1.6))
                steps = [("app_open", {}), ("address_search", {"query_len": R.randint(3, 18), "results": R.randint(1, 8)}),
                         ("price_quote", {"from_zone": ref["zone_of"](b, f[0], f[1]), "service": e["cart"], "quoted_price": quote, "surge": round(PRICE_MULT.get(e["t_accept"].hour, 1.0), 2)})]
                if R.random() < 0.35:
                    steps.append(("price_quote", {"from_zone": ref["zone_of"](b, f[0], f[1]), "service": e["cart"], "quoted_price": int(quote * R.uniform(0.95, 1.1)), "surge": round(PRICE_MULT.get(e["t_accept"].hour, 1.0), 2)}))
                if converted:
                    steps.append(("order_create", {"order_id": e["order_id"], "payment": "card" if e["card"] else "cash", "price": e["s_pay"]}))
                    if e["status"] == 0:
                        steps.append(("order_cancel", {"order_id": e["order_id"], "seconds_after_create": R.randint(5, 240)}))
                    elif e["t_done"] and R.random() < 0.3:
                        steps.append(("rating_submit", {"order_id": e["order_id"], "stars": R.choices([5, 4, 3, 2, 1], weights=[.7, .17, .06, .03, .04])[0]}))
                else:
                    steps.append(("app_background", {}))
                for name, props in steps:
                    t = t + dt.timedelta(seconds=R.randint(2, 45))
                    if t.date() != d:
                        continue
                    ev = {"event_id": f"ev{eid:09d}", "ts": t.strftime("%Y-%m-%dT%H:%M:%S"), "event": name}
                    ev.update(base)
                    ev["props"] = props
                    lines.append((ev["ts"], json.dumps(ev, ensure_ascii=False)))
                    eid += 1
        lines.sort()
        path = f"{out}/app_events/date={d.isoformat()}/events.jsonl"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(l for _, l in lines) + "\n")
        stats[("app_events", "all")] += len(lines)


def gen_chat(extras, out, stats):
    intents = {"CLIENT": ["WHERE_ARE_YOU", "ADDRESS_CLARIFICATION", "DELAY_NOTICE", "THANKS", "OTHER"], "DRIVER": ["ARRIVED", "DELAY_NOTICE", "ADDRESS_CLARIFICATION", "OTHER"]}
    mid = 1
    by_day = defaultdict(list)
    for (b, d), lst in extras.items():
        for e in lst:
            if e["channel"] == "APP" and e["t_busy"] and R.random() < 0.18:
                t = e["t_busy"]
                for _ in range(R.randint(2, 5)):
                    t = t + dt.timedelta(seconds=R.randint(15, 180))
                    sender = R.choice(["CLIENT", "DRIVER"])
                    by_day[d].append((t, json.dumps({"msg_id": f"m{mid:08d}", "order_id": e["order_id"], "branch": b, "ts": t.strftime("%Y-%m-%dT%H:%M:%S"), "sender": sender,
                                                     "intent": R.choice(intents[sender]), "text_len": R.randint(4, 90), "text_redacted": True})))
                    mid += 1
    for d, rows in sorted(by_day.items()):
        rows.sort()
        path = f"{out}/chat/chat_{d:%Y%m%d}.jsonl"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write("\n".join(l for _, l in rows) + "\n")
        stats[("chat", "all")] += len(rows)


def gen_finance(extras, ref, out, stats):
    """General-ledger export in the shape accounting packages produce: cp1251, ';', dd.MM.yyyy, decimal comma, Russian headers."""
    by_month = defaultdict(list)
    names = {"PASSENGER": "Выручка: пассажирские перевозки", "CARGO": "Выручка: грузовые перевозки", "SPECIAL": "Выручка: эвакуация и спецтехника"}
    daily = defaultdict(lambda: defaultdict(float))
    payout = defaultdict(float)
    for (b, d), lst in extras.items():
        for e in lst:
            if e["t_done"]:
                # revenue is booked gross; contractor payouts (fare less commission) are a cost,
                # exactly as in the payment system; own-fleet trips have no payout
                daily[(b, d)][e["line"]] += e["pay"]
                rate = ref["drivers"][b].get(e["driver"], {"commission": 0.2})["commission"]
                if rate:
                    payout[(b, d)] += round(e["pay"] - round(e["pay"] * rate, 2), 2)
    def money(x):
        return f"{x:.2f}".replace(".", ",")
    for (b, d), lines in sorted(daily.items()):
        for line, amount in sorted(lines.items()):
            by_month[d.strftime("%Y-%m")].append([d.strftime("%d.%m.%Y"), f"Филиал {b}", "90.01", names[line], money(amount), "RUB", "Дт 62 Кт 90.01"])
        by_month[d.strftime("%Y-%m")].append([d.strftime("%d.%m.%Y"), f"Филиал {b}", "20.01", "Выплаты водителям-подрядчикам", money(-payout[(b, d)]), "RUB", "Дт 20.01 Кт 60"])
    expenses = [("26", "Заработная плата: колл-центр", 1450000, 780000), ("26", "Заработная плата: ИТ-отдел", 620000, 210000), ("25", "Серверное оборудование и обслуживание", 410000, 265000),
                ("25", "Лицензии и поддержка ПО", 180000, 95000), ("25", "Связь и интернет", 140000, 85000), ("20.02", "Топливо: собственный автопарк", 1900000, 1150000),
                ("20.02", "Ремонт и ТО автопарка", 760000, 480000), ("26", "Аренда офиса", 520000, 310000), ("26", "Электроэнергия серверной", 96000, 61000)]
    for month in sorted(by_month):
        y, m = map(int, month.split("-"))
        last = 15 if (y, m) == (2026, 11) else 28
        frac = 0.5 if (y, m) == (2026, 11) else 1.0
        for acc, name, a_amt, b_amt in expenses:
            for b, amt in (("A", a_amt), ("B", b_amt)):
                by_month[month].append([dt.date(y, m, last).strftime("%d.%m.%Y"), f"Филиал {b}", acc, name, money(-amt * frac * R.gauss(1, 0.03)), "RUB", f"Дт {acc} Кт 60"])
    for month, rows in sorted(by_month.items()):
        write_csv(f"{out}/finance/1c_gl_export_{month}.csv", ["Дата", "Подразделение", "Счет", "Статья", "Сумма", "Валюта", "Проводка"], rows, encoding="cp1251", delimiter=";")
        stats[("gl_rows", "all")] += len(rows)


def gen_external(out, stats):
    path = f"{out}/external/competitor_prices.jsonl"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    n = 0
    with open(path, "w") as fh:
        t = dt.datetime.combine(HIST_START, dt.time(0, 0))
        end = dt.datetime(2026, 11, 16, 10, 30)
        while t <= end:
            for b in "AB":
                for comp, k in (("COMPETITOR_1", 1.0), ("COMPETITOR_2", 0.93), ("COMPETITOR_3", 1.08)):
                    for rc, km in (("SHORT_3KM", 3), ("MEDIUM_8KM", 8), ("LONG_20KM", 20)):
                        surge = PRICE_MULT.get(t.hour, 1.0) * R.gauss(1, 0.06)
                        fh.write(json.dumps({"ts": t.strftime("%Y-%m-%dT%H:%M:%S"), "branch": b, "source": comp, "route_class": rc,
                                             "price": int((230 + 33 * km) * k * surge), "surge": round(surge, 2), "collected_by": "price_monitor_bot"}) + "\n")
                        n += 1
            t += dt.timedelta(minutes=30)
    stats[("competitor_prices", "all")] = n
    rows = []
    t = dt.datetime.combine(HIST_START, dt.time(0, 0))
    while t <= dt.datetime(2026, 11, 16, 10, 0):
        for b in "AB":
            rush = {7: 6.5, 8: 8.0, 9: 6.5, 17: 7.5, 18: 8.5, 19: 6.5}.get(t.hour, 2.0 if t.hour < 6 else 4.0)
            heavy = t.date() in EVENT_DAYS and EVENT_DAYS[t.date()] > 2
            rows.append([ts(t), BRANCHES[b]["id"], round(min(10, max(0, rush * (1.25 if heavy else 1.0) * (0.75 if b == "B" else 1.0) + R.gauss(0, 0.5))), 1)])
        t += dt.timedelta(hours=1)
    write_csv(f"{out}/external/traffic_index.csv", ["ts", "branch_id", "traffic_index"], rows)
    stats[("traffic_rows", "all")] = len(rows)


def gen_gps_b(extras, ref, out, stats):
    """Branch B vehicle positions for the hours covered by the real branch A extract, with the same quirks:
    one-minute cadence, bursts that share a receive minute, lost fixes and -1 placeholders."""
    lst = extras.get(("B", SAMPLE_DAY), [])
    start, end = dt.datetime(2026, 11, 16, 0, 0), dt.datetime(2026, 11, 16, 10, 23)
    legs = defaultdict(list)
    for e in lst:
        if e["driver"] and e["t_busy"]:
            f, t = ref["addr"]["B"][e["from"]], ref["addr"]["B"][e["to"]]
            legs[e["driver"]].append((e["t_busy"], e["t_arr"], e["t_pick"], e["t_done"], f, t))
    idle = R.sample([d for d in ref["drivers"]["B"] if d not in legs], 80)
    box = ref["boxes"]["B"]
    rows, gid = [], 301455120
    own = {d for d, i in ref["drivers"]["B"].items() if i["own"]}

    def emit(t, d, lat, lon, speed, state):
        nonlocal gid
        fix = 0 if R.random() < 0.002 else 1
        sp = -1 if R.random() < 0.004 else (0 if fix == 0 else round(max(0, speed), 2))
        track = -1 if R.random() < 0.007 else (0 if sp <= 0 else round(R.uniform(0, 360), 2))
        rows.append([gid, d, ts(t.replace(second=0)), round(lat, 6), round(lon, 6), 0, sp, track, fix,
                     max(1, int(R.lognormvariate(math.log(5), 0.9))), state, 1052398000 + ref["drivers"]["B"][d]["car_id"] if d in own and R.random() < 0.5 else ""])
        gid += 1

    for d in list(legs) + idle:
        lat, lon = R.uniform(box[0], box[1]), R.uniform(box[2], box[3])
        segs = sorted(legs.get(d, []), key=lambda s: s[0])
        if d in idle:
            on = start + dt.timedelta(minutes=R.randint(0, 520))
            off = on + dt.timedelta(minutes=R.randint(30, 180))
        else:                                   # on line from a while before the first order until a while after the last
            on = max(start, segs[0][0] - dt.timedelta(minutes=R.randint(10, 90)))
            off = max((s[3] or s[2] or s[1] or s[0]) for s in segs) + dt.timedelta(minutes=R.randint(10, 120))
        idle_state = 0 if d in idle and R.random() < 0.55 else 1
        t = on
        while t <= min(off, end):
            state, target = idle_state, None
            for (tb, ta, tp, td, f, to) in segs:
                last = td or tp or ta or tb + dt.timedelta(minutes=8)
                if tb <= t <= last:
                    state = 2
                    if ta and t < ta:
                        target, remain = f, (ta - t).total_seconds() / 60
                    elif tp and td and t >= tp:
                        target, remain = to, (td - t).total_seconds() / 60
                    else:
                        target, remain = f, 0
                    break
            plat, plon = lat, lon
            if target and remain > 0:
                lat += (target[0] - lat) / max(1.0, remain) + R.gauss(0, 0.0003)
                lon += (target[1] - lon) / max(1.0, remain) + R.gauss(0, 0.0005)
            elif state == 1 and R.random() < 0.35:
                lat += R.gauss(0, 0.0012)
                lon += R.gauss(0, 0.002)
            speed = hav_km(plat, plon, lat, lon) * 1000 / 60.0
            if R.random() < 0.03:
                t += dt.timedelta(minutes=R.randint(2, 6))      # connectivity gap, then the terminal flushes its buffer
                for _ in range(R.randint(2, 5)):
                    emit(t, d, lat + R.gauss(0, 0.001), lon + R.gauss(0, 0.0015), speed * R.uniform(0.5, 1.5), state)
            emit(t, d, lat, lon, speed, state)
            t += dt.timedelta(minutes=1)
    rows.sort(key=lambda r: (r[2], r[0]))
    for i, r in enumerate(rows):
        r[0] = 301455120 + i
    write_csv(f"{out}/gps/branch=B/gps_branchB_20261116.csv", ["ID", "COD_DRV", "D_RECEIVE", "LAT", "LON", "SECTOR_ID", "SPEED", "TRACK", "FIX", "QUALITY", "STATE_GPS_ID", "ID_TERMINAL"], rows)
    stats[("gps_rows", "B")] = len(rows)
    stats[("gps_drivers", "B")] = len({r[1] for r in rows})


def main():
    global ORDER_COLS
    real_dir, out = sys.argv[1], sys.argv[2]
    with open(f"{real_dir}/orders_branchA_20261116.csv", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        ORDER_COLS = rd.fieldnames
        real_orders = list(rd)
    with open(f"{real_dir}/gps_branchA_20261116.csv", encoding="utf-8") as f:
        real_gps = list(csv.DictReader(f))

    ref = build_reference(real_orders, real_gps, out)
    extras, stats, seq = gen_orders(ref, real_orders, out)
    gen_payments(extras, ref, out, stats)
    gen_cdr(extras, ref, out, stats, seq)
    gen_sms(extras, out, stats)
    gen_app_events(extras, ref, out, stats)
    gen_chat(extras, out, stats)
    gen_finance(extras, ref, out, stats)
    gen_external(out, stats)
    gen_gps_b(extras, ref, out, stats)

    summary = {f"{k[0]}:{k[1]}": v for k, v in sorted(stats.items(), key=lambda kv: str(kv[0]))}
    with open(f"{out}/_generation_summary.json", "w") as f:
        json.dump(summary, f, indent=1)
    for k, v in summary.items():
        print(f"{k:28s} {v}")


if __name__ == "__main__":
    main()
