#!/usr/bin/env python3
"""Takes the picture of the Superset dashboard for the report: shots/web_dashboard.png.

    python3 shot_dashboard.py <dashboard id>

Logs in, opens the dashboard without the Superset menu, waits until every tile has drawn
(each tile is a Hive query, so this takes a minute or two) and prints how long that took.
"""
import pathlib, sys, time
from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:8090"
OUT = pathlib.Path(__file__).resolve().parent.parent / "shots" / "web_dashboard.png"
dash = sys.argv[1]
with sync_playwright() as p:
    b = p.chromium.launch()
    page = b.new_page(viewport={"width": 1500, "height": 1250}, device_scale_factor=2)
    page.goto(f"{BASE}/login/")
    page.fill("#username", "admin"); page.fill("#password", "prototype-admin")
    page.click("input[type=submit], button[type=submit]")
    page.wait_for_load_state("networkidle")
    t0 = time.time()
    page.goto(f"{BASE}/superset/dashboard/{dash}/?standalone=2")
    page.wait_for_selector(".dashboard-component-chart-holder", timeout=120000)
    n = page.locator(".dashboard-component-chart-holder").count()
    for _ in range(600):
        if page.locator(".dashboard-component-chart-holder .loading, .dashboard-component-chart-holder [role=status]").count() == 0:
            break
        time.sleep(1)
    time.sleep(3)
    errors = page.locator(".dashboard-component-chart-holder [role=alert]").count()
    print(f"{n} tiles drawn in {time.time() - t0:.0f} s, {errors} with an error")
    page.screenshot(path=str(OUT), full_page=True)
    b.close()
print("written", OUT)
