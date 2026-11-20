#!/usr/bin/env python3
"""Builds the prototype dashboard in Apache Superset through its REST API.

    python3 build_dashboard.py

Superset (container lake-superset, see start_superset.sh) connects to HiveServer2 of the lake
and reads the report tables of hive/06_report_tables.hql. Every tile is a Hive query.
Running the script again replaces the dashboard.
"""
import json
import sys
import requests

BASE = "http://127.0.0.1:8090"
HIVE = "hive://bigdata@172.17.0.2:10000/reports"
TITLE = "Transport company: management and demand reports"

s = requests.Session()
tok = s.post(f"{BASE}/api/v1/security/login", json={"username": "admin", "password": "prototype-admin", "provider": "db", "refresh": True}).json()["access_token"]
s.headers.update({"Authorization": f"Bearer {tok}"})


def call(method, path, **kw):
    r = s.request(method, f"{BASE}/api/v1/{path}", **kw)
    if r.status_code >= 300:
        sys.exit(f"{method} {path}: {r.status_code} {r.text[:400]}")
    return r.json()


def find(path, column, value):
    q = json.dumps({"filters": [{"col": column, "opr": "eq", "value": value}]})
    return [x["id"] for x in call("GET", f"{path}/?q={q}")["result"]]


# start clean
for d in find("dashboard", "dashboard_title", TITLE):
    call("DELETE", f"dashboard/{d}")
for c in call("GET", "chart/?q=" + json.dumps({"page_size": 100}))["result"]:
    call("DELETE", f"chart/{c['id']}")
for d in call("GET", "dataset/?q=" + json.dumps({"page_size": 100}))["result"]:
    call("DELETE", f"dataset/{d['id']}")
for d in find("database", "database_name", "Data lake (Hive)"):
    call("DELETE", f"database/{d}")

db = call("POST", "database/", json={"database_name": "Data lake (Hive)", "sqlalchemy_uri": HIVE, "expose_in_sqllab": True})["id"]
ds = {t: call("POST", "dataset/", json={"database": db, "schema": "reports", "table_name": t})["id"]
      for t in ("rpt_monthly_revenue", "rpt_orders_hour", "rpt_price_km", "rpt_fleet_hour")}
dash = call("POST", "dashboard/", json={"dashboard_title": TITLE, "published": True})["id"]


def metric(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label, "optionName": "metric_" + label.replace(" ", "_")}


def keep(column, values):
    return {"expressionType": "SIMPLE", "clause": "WHERE", "subject": column, "operator": "IN", "comparator": values}


def bar(x, metrics, groupby=(), stack=False, y_title="", filters=(), fmt="SMART_NUMBER"):
    return {"x_axis": x, "metrics": metrics, "groupby": list(groupby), "stack": "Stack" if stack else None, "row_limit": 1000,
            "x_axis_sort_asc": True, "show_legend": True, "legendOrientation": "top", "orientation": "vertical", "show_value": True,
            "y_axis_title": y_title, "y_axis_title_margin": 40, "y_axis_format": fmt, "adhoc_filters": list(filters),
            "color_scheme": "supersetColors", "truncateXAxis": False, "xAxisLabelRotation": 0, "rich_tooltip": True}


def big(m, subheader):
    return {"metric": m, "subheader": subheader, "y_axis_format": "SMART_NUMBER", "header_font_size": 0.4, "subheader_font_size": 0.15, "adhoc_filters": []}


CANC = "SUM(CASE WHEN outcome = 'Cancelled' THEN orders ELSE 0 END) * 100.0 / SUM(orders)"
charts = [
    # name, table, viz, params, width (of 12), height
    ("Orders, branch A, 16 Nov 00:00-10:11", "rpt_orders_hour", "big_number_total", big(metric("SUM(orders)", "Orders"), "real extract"), 3, 22),
    ("Cancelled, % of orders", "rpt_orders_hour", "big_number_total", big(metric(CANC, "Cancelled %"), "all channels") | {"y_axis_format": ".1f"}, 3, 22),
    ("Cancelled, % of app orders", "rpt_orders_hour", "big_number_total",
     big(metric(CANC, "Cancelled %"), "mobile app") | {"y_axis_format": ".1f", "adhoc_filters": [keep("channel", ["Mobile app"])]}, 3, 22),
    ("Flagged: no car in time", "rpt_orders_hour", "big_number_total", big(metric("SUM(flagged_no_car_in_time)", "Flagged"), "orders"), 3, 22),
    ("Orders by hour and outcome (real extract)", "rpt_orders_hour", "echarts_timeseries_bar",
     bar("order_hour", [metric("SUM(orders)", "Orders")], ["outcome"], stack=True, y_title="orders"), 6, 50),
    ("Cancelled share by channel and hours (real extract)", "rpt_orders_hour", "echarts_timeseries_bar",
     bar("hours", [metric(CANC, "Cancelled %")], ["channel"], y_title="% of orders", filters=[keep("channel", ["Mobile app", "Call centre"])], fmt=".1f"), 6, 50),
    ("Median price per km, night against morning peak (real extract)", "rpt_price_km", "echarts_timeseries_bar",
     bar("trip_length", [metric("MAX(median_rub_per_km)", "Roubles per km")], ["period"], y_title="roubles per km", fmt=".0f"), 6, 50),
    ("Share of on-shift time spent on an order, by hour (real positions)", "rpt_fleet_hour", "echarts_timeseries_bar",
     bar("hour_of_day", [metric("MAX(on_order_pct)", "On an order, %")], y_title="%", fmt=".0f"), 6, 50),
    ("Revenue by month and city (generated history)", "rpt_monthly_revenue", "echarts_timeseries_bar",
     bar("year_month", [metric("SUM(revenue_rub)", "Revenue, roubles")], ["city"], y_title="roubles"), 5, 50),
    ("Monthly management report (generated history)", "rpt_monthly_revenue", "table",
     {"query_mode": "aggregate", "groupby": ["year_month", "city", "service_line"],
      "metrics": [metric("SUM(orders)", "Orders"), metric("SUM(completed)", "Completed"), metric("SUM(cancelled)", "Cancelled"), metric("SUM(revenue_rub)", "Revenue, roubles")],
      "row_limit": 100, "order_desc": False, "table_timestamp_format": "smart_date", "adhoc_filters": [], "page_length": 0, "include_search": False}, 7, 50),
]

position = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
            "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": TITLE}}}
row, used = None, 12
for i, (name, table, viz, params, width, height) in enumerate(charts):
    params = dict(params, viz_type=viz, datasource=f"{ds[table]}__table")
    cid = call("POST", "chart/", json={"slice_name": name, "viz_type": viz, "datasource_id": ds[table], "datasource_type": "table",
                                        "params": json.dumps(params), "dashboards": [dash]})["id"]
    if used + width > 12:
        row = f"ROW-{len(position)}"; used = 0
        position[row] = {"type": "ROW", "id": row, "children": [], "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        position["GRID_ID"]["children"].append(row)
    key = f"CHART-{i}"
    position[key] = {"type": "CHART", "id": key, "children": [], "parents": ["ROOT_ID", "GRID_ID", row],
                     "meta": {"chartId": cid, "width": width, "height": height, "sliceName": name}}
    position[row]["children"].append(key); used += width
call("PUT", f"dashboard/{dash}", json={"position_json": json.dumps(position)})
print(f"dashboard {dash} built with {len(charts)} tiles: {BASE}/superset/dashboard/{dash}/")
