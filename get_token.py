"""Capture a live Suno bearer token from the logged-in browser profile and
write it to ~/.suno-midi/bearer.txt for the downloader to use."""
from __future__ import annotations
from pathlib import Path
from playwright.sync_api import sync_playwright

PROFILE = str(Path.home() / ".suno-midi" / "profile")
OUT = Path.home() / ".suno-midi" / "bearer.txt"


def main():
    token = {}
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=PROFILE, channel="chrome", headless=False,
            viewport={"width": 1200, "height": 800},
            args=["--window-position=0,0", "--hide-crash-restore-bubble"])
        page = ctx.pages[0] if ctx.pages else ctx.new_page()

        def grab(r):
            a = r.headers.get("authorization", "")
            if "studio-api" in r.url and a.startswith("Bearer "):
                token["t"] = a[7:]
        ctx.on("request", grab)

        page.goto("https://suno.com/me", wait_until="commit", timeout=60000)
        for _ in range(30):
            page.wait_for_timeout(1500)
            if token.get("t"):
                break
        if not token.get("t"):
            page.reload(wait_until="commit")
            for _ in range(20):
                page.wait_for_timeout(1500)
                if token.get("t"):
                    break
        ctx.close()

    if token.get("t"):
        OUT.write_text(token["t"])
        print(f"token captured ({len(token['t'])} chars) -> {OUT}")
    else:
        print("NO TOKEN CAPTURED")


if __name__ == "__main__":
    main()
