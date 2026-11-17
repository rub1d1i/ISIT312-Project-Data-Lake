#!/usr/bin/env python3
"""
Builds the files of the defect test (run by inject_defects.sh).

The first night's data has no orphan keys and no re-sent order versions, so the
defensive steps of the load never ran. This test adds, as a second night would:
  1. an order of a client that is not in the client table      (fare 500)
  2. an order of a driver that is not in the driver table      (fare 700)
  3. a newer version of an existing order (status changed one minute later)
  4. a client registered today, who must get a new warehouse key
Orders 1 and 2 are completed and have no payment, so the control totals of the
warehouse must show a difference of exactly 1,200.00 in branch A.

Usage: python3 make_defects.py <synthetic_dir> <out_dir>
"""
import csv
import datetime as dt
import os
import sys

src, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
with open(f"{src}/orders/branch=A/orders_A_20261115.csv", encoding="utf-8") as f:
    rd = csv.DictReader(f)
    cols = rd.fieldnames
    done = [r for r in rd if r["STATUS"] == "6" and r["D_VIPOL"] and r["COD_DRV"]]
r1, r2, r3 = dict(done[0]), dict(done[1]), dict(done[2])
r1.update(ORDER_ID="76990001", COD_CL="9999901", PAY="500", S_PAY="500")
r2.update(ORDER_ID="76990002", COD_DRV="999901", PAY="700", S_PAY="700")
later = dt.datetime.strptime(r3["DS_STATUS"], "%Y-%m-%d %H:%M:%S") + dt.timedelta(minutes=1)
r3.update(DS_STATUS=later.strftime("%Y-%m-%d %H:%M:%S"), ZAKAZ_INFO="CALLBACK_REQUEST")
with open(f"{out}/orders_A_20261115_second_night.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    w.writerows([r1, r2, r3])
with open(f"{out}/clients_delta_20261116.csv", "w", newline="") as f:
    f.write("cod_cl,branch_id,client_type,corp_account_id,app_user,first_seen_date\n9999902,1,PRIVATE,,1,2026-11-16\n")
print("order 1: client 9999901 (unknown), fare 500, order", r1["ORDER_ID"])
print("order 2: driver 999901 (unknown), fare 700, order", r2["ORDER_ID"])
print("order 3: newer version of order", r3["ORDER_ID"], "status changed at", r3["DS_STATUS"])
print("new client: 9999902 (branch A)")
