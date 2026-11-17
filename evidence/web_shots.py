#!/usr/bin/env python3
"""
Screenshots of the web consoles of the running lake (NameNode, YARN, HBase),
taken with a headless browser from the ports the container publishes on
localhost. Run on the Mac while the lake is up:

  python3 web_shots.py [name ...]        # no names = all
"""
import sys

from playwright.sync_api import sync_playwright

SHOTS = {
    # name: (url, text to wait for, viewport height)
    "web_namenode_overview": ("http://localhost:50070/dfshealth.html#tab-overview", "Configured Capacity", 1250),
    "web_hdfs_lake": ("http://localhost:50070/explorer.html#/lake", "analytics", 560),
    "web_hdfs_raw_denied": ("http://localhost:50070/explorer.html#/lake/raw", "Permission denied", 330),
    "web_hdfs_dw": ("http://localhost:50070/explorer.html#/lake/analytics/dw", "fact_orders", 980),
    "web_hdfs_fact_orders": ("http://localhost:50070/explorer.html#/lake/analytics/dw/fact_orders", "branch_code", 480),
    "web_yarn_apps": ("http://localhost:8088/cluster/apps", "application_", 900),
    "web_hbase_master": ("http://localhost:16010/master-status", "Region Servers", 1000),
}


def main():
    wanted = sys.argv[1:] or list(SHOTS)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for name in wanted:
            url, wait_text, height = SHOTS[name]
            page = browser.new_page(viewport={"width": 1500 if "yarn" in name else 1280, "height": height}, device_scale_factor=2)
            page.goto(url, wait_until="networkidle", timeout=30000)
            try:
                page.get_by_text(wait_text, exact=False).first.wait_for(timeout=10000)
            except Exception as e:                      # still take the picture, but say so
                print(f"  note: '{wait_text}' not found on {url}: {type(e).__name__}")
            page.wait_for_timeout(600)
            page.screenshot(path=f"shots/{name}.png")
            page.close()
            print("captured", name)
        browser.close()


if __name__ == "__main__":
    main()
