"""Ambil bukti teknis ke docs/: redpanda_stream.png dan minio_lake.png (butuh Microsoft Edge + playwright)."""
import re
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

DOCS = Path(__file__).resolve().parent.parent / "docs"
CONSOLE = "http://localhost:8080"
S3UI = "http://localhost:8081"
BUCKET = "crypto-lake"


def links(url):
    html = urllib.request.urlopen(url, timeout=10).read().decode()
    return re.findall(r'href="(/Default/buckets/[^"]+)"', html)


def find_parquet_folder():
    """Turun dari raw/ sampai folder yang berisi file .parquet."""
    path = f"/Default/buckets/{BUCKET}/raw/"
    for _ in range(4):
        found = [l for l in links(S3UI + path) if l.startswith(path) and l != path]
        if not found:
            break
        if any(".parquet" in l for l in found):
            return path
        folders = [l for l in found if l.endswith("/")]
        if not folders:
            break
        path = sorted(folders)[0]
    return path


DOCS.mkdir(exist_ok=True)
with sync_playwright() as p:
    browser = p.chromium.launch(channel="msedge", headless=True)
    page = browser.new_page(viewport={"width": 1600, "height": 1000})

    page.goto(f"{CONSOLE}/topics/crypto_ticks")
    page.wait_for_timeout(6000)
    page.screenshot(path=str(DOCS / "redpanda_stream.png"))

    folder = find_parquet_folder()
    print("parquet folder:", folder)
    page.set_viewport_size({"width": 1700, "height": 760})
    page.goto(S3UI + folder)
    page.wait_for_timeout(2500)
    # tampilkan nama file lengkap (UI memotong dengan "...") - hanya efek tampilan
    page.evaluate("""() => {
      document.querySelectorAll('*').forEach(el => {
        if (el.children.length === 0 && (el.textContent || '').trim().startsWith('part-')) {
          el.style.cssText += ';overflow:visible;text-overflow:clip;white-space:nowrap;max-width:none;width:auto;display:inline-block';
          let p = el.parentElement;
          for (let i = 0; i < 4 && p; i++, p = p.parentElement) {
            p.style.cssText += ';overflow:visible;text-overflow:clip;max-width:none;flex-shrink:0';
          }
        }
      });
    }""")
    page.wait_for_timeout(500)
    page.screenshot(path=str(DOCS / "minio_lake.png"))
    browser.close()
print("saved to", DOCS)
