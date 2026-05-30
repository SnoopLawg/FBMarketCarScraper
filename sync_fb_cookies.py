#!/usr/bin/env python3
"""Zero-touch FB cookie sync.

Reads facebook.com cookies from your normal logged-in Firefox profile via
`browser_cookie3` (which bypasses the httpOnly wall — `xs`/`c_user` are
included), converts them to the scraper's pickle format, and scp's the
result to the prod server.

Combined with the `_save_cookies()` fix that runs after every scrape, this
means: stay logged in to FB *anywhere* in your normal Firefox and the
scraper's session auto-renews forever — no interactive refresh needed.

Run manually:
    venv/bin/python sync_fb_cookies.py             # full sync (default)
    venv/bin/python sync_fb_cookies.py --dry-run   # read only, no write
    venv/bin/python sync_fb_cookies.py --no-upload # save pickle locally only

Install once, then let launchd run it every 12h:
    pip install -r requirements-dev.txt   # adds browser_cookie3
    cp launchd/com.snoop.fb-cookie-sync.plist ~/Library/LaunchAgents/
    launchctl load ~/Library/LaunchAgents/com.snoop.fb-cookie-sync.plist
"""
import argparse
import pickle
import subprocess
import sys
from pathlib import Path

# Without these two cookies FB serves the anon view (~30% of real yield).
REQUIRED_AUTH = {"c_user", "xs"}

DEFAULT_SERVER = "snoop@192.168.0.244"
DEFAULT_REMOTE = "/home/snoop/docker/carscraper/data/fb_cookies.pkl"
LOCAL_OUT = Path(__file__).parent / "fb_cookies.pkl"


def cookies_to_selenium_format(cookiejar):
    """Convert a `http.cookiejar.CookieJar` (what browser_cookie3 returns)
    into the list-of-dicts format `driver.get_cookies()` produces, which is
    what `scrapers/facebook.py:_load_cookies` expects after unpickling.
    """
    out = []
    for c in cookiejar:
        out.append({
            "name": c.name,
            "value": c.value,
            "domain": c.domain,
            "path": c.path,
            "secure": bool(c.secure),
            "expiry": int(c.expires) if c.expires else None,
        })
    return out


def read_firefox_cookies():
    """Read FB cookies from the user's Firefox profile. Returns the
    selenium-shaped list of cookie dicts, or raises on failure."""
    import browser_cookie3
    jar = browser_cookie3.firefox(domain_name="facebook.com")
    return cookies_to_selenium_format(jar)


def main(argv=None):
    """argv=None → read from sys.argv. Pass an explicit list to call from tests."""
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--server", default=DEFAULT_SERVER,
                   help=f"scp target (default: {DEFAULT_SERVER})")
    p.add_argument("--remote-path", default=DEFAULT_REMOTE,
                   help=f"remote pickle path (default: {DEFAULT_REMOTE})")
    p.add_argument("--no-upload", action="store_true",
                   help="Save pickle locally only; don't scp")
    p.add_argument("--dry-run", action="store_true",
                   help="Read & validate cookies but don't write or upload")
    args = p.parse_args(argv)

    try:
        cookies = read_firefox_cookies()
    except ImportError:
        print("✗ browser_cookie3 not installed. "
              "Run: pip install -r requirements-dev.txt", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"✗ Couldn't read Firefox cookies: {e}", file=sys.stderr)
        return 1

    names = {c["name"] for c in cookies}
    missing = REQUIRED_AUTH - names
    if missing:
        print(f"✗ Missing required auth cookies: {sorted(missing)}\n"
              "  -> Are you actively logged in to facebook.com in your "
              "Firefox right now?", file=sys.stderr)
        return 1

    print(f"✓ Read {len(cookies)} FB cookies (incl. {sorted(REQUIRED_AUTH)})")

    if args.dry_run:
        print("(--dry-run; nothing written)")
        return 0

    with open(LOCAL_OUT, "wb") as f:
        pickle.dump(cookies, f)
    print(f"✓ Saved pickle: {LOCAL_OUT} "
          f"({LOCAL_OUT.stat().st_size // 1024} KB)")

    if args.no_upload:
        return 0

    print(f"Uploading to {args.server}:{args.remote_path} …")
    r = subprocess.run(
        ["scp", "-o", "ConnectTimeout=10",
         str(LOCAL_OUT), f"{args.server}:{args.remote_path}"],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        print(f"✗ scp failed: {r.stderr.strip()}\n"
              f"  Cookies still saved locally at {LOCAL_OUT}",
              file=sys.stderr)
        return 1
    print("✓ Uploaded. Next scrape will pick them up automatically.")
    return 0


# Alias so tests don't depend on argparse defaulting to sys.argv.
main_for_test = main


if __name__ == "__main__":
    sys.exit(main())
