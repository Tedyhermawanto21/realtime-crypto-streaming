"""Ambil screenshot dashboard Grafana ke docs/grafana-dashboard.png (butuh Microsoft Edge/Chrome terpasang).

Kredensial dibaca dari env: GRAFANA_USER (default admin), GRAFANA_PASSWORD (default admin).
"""
import os
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = os.getenv("GRAFANA_URL", "http://localhost:3000")
USER = os.getenv("GRAFANA_USER", "admin")
PASSWORD = os.getenv("GRAFANA_PASSWORD", "admin")
OUT = Path(__file__).resolve().parent.parent / "docs" / "grafana-dashboard.png"
URL = f"{BASE}/d/crypto-1m/crypto-realtime-1-minute-metrics?orgId=1&from=now-30m&to=now&kiosk"

with sync_playwright() as p:
    browser = p.chromium.launch(channel="msedge", headless=True)
    page = browser.new_page(viewport={"width": 1600, "height": 1100})
    page.goto(f"{BASE}/login")
    page.fill("input[name='user']", USER)
    page.fill("input[name='password']", PASSWORD)
    page.click("button[type='submit']")
    page.wait_for_timeout(2000)
    skip = page.get_by_text("Skip", exact=True)  # layar "Update your password"
    if skip.count():
        skip.first.click()
    page.wait_for_url(lambda u: "/login" not in u, timeout=15000)
    page.goto(URL)
    page.wait_for_timeout(8000)  # tunggu panel selesai render
    OUT.parent.mkdir(exist_ok=True)
    page.screenshot(path=str(OUT), full_page=True)
    browser.close()
print("saved", OUT)
