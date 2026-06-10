#!/usr/bin/env python3
"""One-time bootstrap for the persistent Facebook scraper profile.

Opens a VISIBLE Firefox using an in-place persistent profile directory
(FB_PROFILE_DIR, default ./fb_profile) and waits for you to log in to
Facebook once. After login, the profile keeps its own consistent
datr+xs device/session cookies ON DISK — so every subsequent headless
scrape run is already logged in, with no pickle juggling and no repeated
challenges. You only ever redo this if FB hard-invalidates the session.

Usage:
    venv/bin/python bootstrap_fb_profile.py
    venv/bin/python bootstrap_fb_profile.py --profile-dir /path/to/fb_profile

On the SERVER (headless) you don't run this — set FB_EMAIL / FB_PASSWORD /
FB_TOTP_SECRET and the scraper's _auto_login() seeds the profile itself on
the first run, then the persistent profile keeps it alive.
"""
import argparse
import os
import sys
import time
from pathlib import Path

# Force a visible window even if HEADLESS leaked into the env
os.environ.pop("HEADLESS", None)
os.environ.pop("DOCKER_MODE", None)

import pickle                                          # noqa: E402
from driver import create_driver                       # noqa: E402
from scrapers.facebook import FacebookScraper, COOKIE_FILE   # noqa: E402

LOGIN_TIMEOUT_SEC = 600   # 10 min — generous for 2FA / checkpoint flows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile-dir",
                    default=os.environ.get("FB_PROFILE_DIR",
                                           str(Path(__file__).parent / "fb_profile")))
    args = ap.parse_args()

    prof = Path(args.profile_dir).resolve()
    print(f"Persistent profile: {prof}")
    print("Opening Firefox — log in to Facebook in the window that appears.")

    driver = create_driver(persistent_profile=str(prof))
    scraper = FacebookScraper(driver, {"MinPrice": 0, "MaxPrice": 1,
                                       "Sources": {"facebook": {}}},
                              insert_fn=lambda **kw: None, car_list=["_"])
    try:
        driver.get("https://www.facebook.com/login")
        print(f"Waiting up to {LOGIN_TIMEOUT_SEC}s for you to log in...")
        deadline = time.time() + LOGIN_TIMEOUT_SEC
        while time.time() < deadline:
            # _is_logged_in() now requires the c_user cookie, so it only
            # fires on a genuinely authenticated session (not a captcha /
            # 2FA / picker interstitial).
            if scraper._is_logged_in():
                # Double-check the auth cookies are actually present before
                # we trust it — guards against any transitional state.
                cookies = {c["name"] for c in driver.get_cookies()}
                if not {"c_user", "xs"} <= cookies:
                    time.sleep(3)
                    continue
                print("✓ Logged in — c_user + xs present.")
                # Touch the home page once so FB writes its cookies to disk
                driver.get("https://www.facebook.com/")
                time.sleep(3)
                # Capture the fresh session to the backup pickle (with a
                # forced future expiry) so headless runs can restore it
                # without ever hitting /login (which triggers reCAPTCHA).
                scraper._save_cookies()
                with open(COOKIE_FILE, "rb") as fh:
                    saved = {c["name"] for c in pickle.load(fh)}
                ok = {"c_user", "xs"} <= saved
                print(f"✓ Saved backup cookies (c_user+xs present: {ok}).")
                break
            time.sleep(3)
        else:
            print("✗ Timed out waiting for login.", file=sys.stderr)
            return 1
    finally:
        driver.quit()   # clean shutdown flushes cookies.sqlite

    print(f"\nDone. Point the scraper at this profile:\n"
          f"    export FB_PROFILE_DIR={prof}\n"
          f"On the server, bind-mount it under /data and set "
          f"FB_PROFILE_DIR=/data/fb_profile in docker-compose.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
