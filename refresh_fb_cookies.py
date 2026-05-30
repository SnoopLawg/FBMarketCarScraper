#!/usr/bin/env python3
"""Interactive helper to refresh fb_cookies.pkl after a session expires.

Opens a real (non-headless) Firefox using the project's existing driver.py
factory — so it picks up the dedicated `6kmbn0d4.fbscraper` profile with
your saved password/2FA, and the fingerprint matches the scraper's. Waits
for you to log in, saves cookies to a local pickle, and scp's it to the
prod server's data dir.

Usage:
    venv/bin/python refresh_fb_cookies.py
        # uses defaults: snoop@192.168.0.244:/home/snoop/docker/carscraper/data/fb_cookies.pkl
    venv/bin/python refresh_fb_cookies.py --no-upload
        # save locally only (./fb_cookies.pkl); don't scp
    venv/bin/python refresh_fb_cookies.py --server user@host --remote-path /path/to/fb_cookies.pkl

The running container picks up the new cookies on its next scheduled scrape
(no restart needed — the scraper re-loads them from disk every run).
"""
import argparse
import logging
import os
import pickle
import subprocess
import sys
import time
from pathlib import Path

# Make sure we open a visible window even if HEADLESS leaked into the env
os.environ.pop("HEADLESS", None)
os.environ.pop("DOCKER_MODE", None)

# Imports after env scrub so driver.py picks up the right values
from driver import create_driver                       # noqa: E402
from scrapers.facebook import FacebookScraper          # noqa: E402

DEFAULT_SERVER = "snoop@192.168.0.244"
DEFAULT_REMOTE = "/home/snoop/docker/carscraper/data/fb_cookies.pkl"
LOCAL_OUT = Path(__file__).parent / "fb_cookies.pkl"
LOGIN_TIMEOUT_SEC = 600   # 10 min — generous for 2FA / checkpoint flows
POLL_INTERVAL = 3
STABILITY_CONFIRM_SEC = 4  # require login signal to hold for this long


def wait_for_login(scraper):
    """Poll until _is_logged_in() returns True for STABILITY_CONFIRM_SEC
    consecutive seconds (avoids false-positives on transient redirects)."""
    deadline = time.time() + LOGIN_TIMEOUT_SEC
    stable_since = None
    while time.time() < deadline:
        if scraper._is_logged_in():
            if stable_since is None:
                stable_since = time.time()
            elif time.time() - stable_since >= STABILITY_CONFIRM_SEC:
                return True
        else:
            stable_since = None
        time.sleep(POLL_INTERVAL)
    return False


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--server", default=DEFAULT_SERVER,
                   help=f"SSH target for upload (default: {DEFAULT_SERVER})")
    p.add_argument("--remote-path", default=DEFAULT_REMOTE,
                   help=f"Remote path (default: {DEFAULT_REMOTE})")
    p.add_argument("--no-upload", action="store_true",
                   help="Save locally only; don't scp")
    args = p.parse_args()

    logging.basicConfig(level=logging.WARNING)
    print("Opening Firefox (using the 6kmbn0d4.fbscraper profile)…")
    drv = create_driver()  # honors the FB-dedicated profile from driver.py

    # Throwaway scraper instance just to reuse its strengthened
    # _is_logged_in() — same detector the scraper itself will run.
    fake_config = {"MinPrice": 5000, "MaxPrice": 30000}
    sc = FacebookScraper(drv, fake_config, lambda **kw: None, car_list=["_"])

    try:
        drv.get("https://www.facebook.com/")
        print()
        print("=" * 60)
        print("  Log in to FB in the Firefox window that just opened.")
        print("  Take your time — 2FA, password manager, checkpoint OK.")
        print(f"  Polling for {LOGIN_TIMEOUT_SEC // 60} minutes…")
        print("=" * 60)

        if not wait_for_login(sc):
            print("\n✗ Timeout waiting for login. Aborting (nothing saved).")
            return 1

        cookies = drv.get_cookies()
        with open(LOCAL_OUT, "wb") as f:
            pickle.dump(cookies, f)
        size_kb = LOCAL_OUT.stat().st_size / 1024
        print(f"\n✓ Login detected. Saved {len(cookies)} cookies "
              f"to {LOCAL_OUT} ({size_kb:.1f} KB)")

        if args.no_upload:
            print("  (--no-upload set; skipping scp)")
            return 0

        print(f"Uploading to {args.server}:{args.remote_path} …")
        result = subprocess.run(
            ["scp", "-o", "ConnectTimeout=10",
             str(LOCAL_OUT), f"{args.server}:{args.remote_path}"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"✗ scp failed: {result.stderr.strip()}")
            print(f"  Cookies are saved locally at {LOCAL_OUT} — upload manually.")
            return 1

        print("✓ Uploaded. The next scheduled scrape will pick them up "
              "automatically (no container restart needed).")
        return 0
    finally:
        try:
            drv.quit()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
