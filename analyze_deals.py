#!/usr/bin/env python3
"""Pull top deals for AI analysis and apply corrections from findings.

Usage:
    # Sync production DB, then pull top 20 deals as JSON
    python analyze_deals.py pull [--count 20]

    # Apply corrections (pushes SQL live to prod — no downtime)
    python analyze_deals.py fix <href> [--price 13000] [--title_type rebuilt] [--notes "reason"]

    # Soft-delete a listing (pushes live to prod — no downtime)
    python analyze_deals.py pass <href> [--notes "rebuilt title, overpriced"]

    # Favorite a BUY-recommended listing (syncs to prod)
    python analyze_deals.py fav <href> --notes "best value Escape, hail damage only"

    # Batch mode: skip deleted_listings.txt sync (SQL still pushes live)
    python analyze_deals.py fix <href> --title_type rebuilt --no-sync
    python analyze_deals.py pass <href> --notes "reason" --no-sync

    # Push deleted_listings.txt to prod (after batch --no-sync operations)
    python analyze_deals.py push

    # Fetch a KSL listing detail page via HTTP (bypasses bot detection)
    python analyze_deals.py fetch <ksl_url>

    # Just sync the DB from production (no analysis)
    python analyze_deals.py sync
"""

import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

import requests

from config import load_config, get_all_search_queries
from database import Database
from analysis import clean_listings, calculate_averages, find_deals, title_group

PROD_HOST = "mothership2"
PROD_DB_PATH = "/home/snoop/docker/carscraper/data/marketplace_listings.db"

logging.basicConfig(level=logging.WARNING)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).parent))
LOCAL_DB = DATA_DIR / "marketplace_listings.db"


def sync_from_prod():
    """Pull the production DB and state files from mothership2 via scp."""
    prod_data_dir = str(Path(PROD_DB_PATH).parent)
    files = [
        (f"{PROD_HOST}:{PROD_DB_PATH}", str(LOCAL_DB)),
        (f"{PROD_HOST}:{prod_data_dir}/deleted_listings.txt",
         str(DATA_DIR / "deleted_listings.txt")),
        (f"{PROD_HOST}:{prod_data_dir}/favorite_listings.txt",
         str(DATA_DIR / "favorite_listings.txt")),
    ]
    print(f"Syncing from {PROD_HOST}...")
    for remote, local in files:
        name = Path(local).name
        result = subprocess.run(
            ["scp", remote, local],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            # Non-fatal for state files (may not exist yet)
            if name == "marketplace_listings.db":
                print(f"ERROR: scp {name} failed: {result.stderr.strip()}")
                sys.exit(1)
        else:
            size_kb = Path(local).stat().st_size / 1024
            print(f"  {name}: {size_kb:.0f} KB")


def _ssh(cmd):
    """Run a command on the production host via SSH."""
    result = subprocess.run(
        ["ssh", PROD_HOST, cmd],
        capture_output=True, text=True, timeout=30,
    )
    return result.returncode == 0, result.stderr.strip()


def _docker_exec(cmd):
    """Run a command inside the production container via SSH + docker exec."""
    full = f'docker exec carscraper {cmd}'
    return _ssh(full)


def _apply_sql_on_prod(sql, params_desc=""):
    """Execute a SQL statement directly on the production DB without downtime.

    Runs the SQL inside the running container via docker exec + python.
    Uses base64 encoding to avoid shell quoting nightmares.
    """
    import base64
    b64 = base64.b64encode(sql.encode()).decode()
    py_cmd = (
        f'python3 -c "'
        f"import sqlite3,base64; "
        f"sql=base64.b64decode('{b64}').decode(); "
        f"c=sqlite3.connect('/data/marketplace_listings.db'); "
        f"c.execute(sql); c.commit(); c.close(); "
        f'print(\'OK\')"'
    )
    ok, err = _docker_exec(py_cmd)
    if ok:
        print(f"  Applied on prod{': ' + params_desc if params_desc else ''}")
    else:
        print(f"  WARNING: prod SQL failed: {err}")
    return ok


def sync_to_prod():
    """Push deleted_listings.txt to production (no container restart needed).

    Individual fix/pass commands push their SQL changes live via
    _apply_sql_on_prod(). This only needs to sync the deleted_listings.txt
    file, which can be copied while the container is running.
    """
    prod_data_dir = str(Path(PROD_DB_PATH).parent)

    deleted_file = DATA_DIR / "deleted_listings.txt"
    if deleted_file.exists():
        result = subprocess.run(
            ["scp", str(deleted_file),
             f"{PROD_HOST}:{prod_data_dir}/deleted_listings.txt"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"ERROR: scp deleted_listings.txt failed: "
                  f"{result.stderr.strip()}")
        else:
            print("Pushed deleted_listings.txt to production.")


def pull_top_deals(count=20):
    """Pull top N active deals with full details for AI inspection."""
    config = load_config()
    db = Database()
    db.open()

    all_cars = get_all_search_queries(config)
    deals = find_deals(db, all_cars, config)

    # Load dismissed listings
    deleted = set()
    favs = set()
    for fname, target in [("deleted_listings.txt", deleted),
                          ("favorite_listings.txt", favs)]:
        p = DATA_DIR / fname
        if p.exists():
            target.update(l.strip() for l in p.read_text().splitlines()
                          if l.strip())

    # Filter to active only, sort by score
    active = [d for d in deals
              if d["href"] not in deleted and d["href"] not in favs]
    active.sort(key=lambda d: d.get("deal_score", 0), reverse=True)
    top = active[:count]

    # Build output with key fields
    results = []
    for d in top:
        results.append({
            "rank": len(results) + 1,
            "car_name": d.get("car_name", ""),
            "car_query": d.get("car_query", ""),
            "year": d.get("year"),
            "price": d.get("price"),
            "avg_price": d.get("avg_price"),
            "deal_score": d.get("deal_score"),
            "deal_grade": d.get("deal_grade"),
            "mileage": d.get("mileage"),
            "source": d.get("source"),
            "location": d.get("location", ""),
            "title_type": d.get("title_type", ""),
            "drivetrain": d.get("drivetrain", ""),
            "trim": d.get("trim", ""),
            "seller": d.get("seller", ""),
            "seller_type": d.get("seller_type", ""),
            "owner_count": d.get("owner_count"),
            "accident_history": d.get("accident_history", ""),
            "vin": d.get("vin", ""),
            "href": d.get("href", ""),
        })

    db.close()
    return results


def apply_fix(href, **kwargs):
    """Apply corrections to a listing based on analysis findings.

    Accepted kwargs: price, title_type, year, car_name, accident_history,
                     condition, seller_type, notes
    """
    db = Database()
    db.open()

    allowed = {
        "title_type", "accident_history", "condition", "seller_type",
    }

    # Fields that need direct SQL updates (not in update_listing_details)
    direct_fields = {}
    detail_fields = {}

    for k, v in kwargs.items():
        if k == "notes":
            continue  # Just for logging
        if k in ("price", "year", "car_name"):
            direct_fields[k] = v
        elif k in allowed:
            detail_fields[k] = v

    # Apply locally
    all_fields = {}
    all_fields.update(direct_fields)
    all_fields.update(detail_fields)

    if direct_fields:
        sets = [f"{k} = ?" for k in direct_fields]
        sets.append("updated_at = CURRENT_TIMESTAMP")
        vals = list(direct_fields.values()) + [href]
        db.cur.execute(
            f"UPDATE listings SET {', '.join(sets)} WHERE href = ?", vals)
        db.conn.commit()

    if detail_fields:
        db.update_listing_details(href, **detail_fields)

    notes = kwargs.get("notes", "")
    fields_list = list(direct_fields.keys()) + list(detail_fields.keys())
    print(f"Updated {href[:60]}... "
          f"fields={fields_list} notes={notes}")

    # Apply each field change live on production (no downtime)
    if all_fields:
        sets = ", ".join(
            f"{k} = '{v}'" if isinstance(v, str)
            else f"{k} = {v}"
            for k, v in all_fields.items()
        )
        sql = (f"UPDATE listings SET {sets}, "
               f"updated_at = CURRENT_TIMESTAMP "
               f"WHERE href = '{href}'")
        _apply_sql_on_prod(sql, f"fix {fields_list}")

    db.close()


def fav_deal(href, notes=""):
    """Favorite a listing (add to favorite_listings.txt, syncs to prod)."""
    fav_file = DATA_DIR / "favorite_listings.txt"
    # Check if already favorited
    existing = set()
    if fav_file.exists():
        existing = {l.strip() for l in fav_file.read_text().splitlines()
                    if l.strip()}
    if href in existing:
        print(f"FAV (already): {href[:60]}...")
        return

    with open(fav_file, "a") as f:
        f.write(href + "\n")

    print(f"FAV: {href[:60]}... {notes}")

    # Push favorite_listings.txt to production
    prod_data_dir = str(Path(PROD_DB_PATH).parent)
    result = subprocess.run(
        ["scp", str(fav_file),
         f"{PROD_HOST}:{prod_data_dir}/favorite_listings.txt"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"  WARNING: scp favorite_listings.txt failed: "
              f"{result.stderr.strip()}")
    else:
        print("Pushed favorite_listings.txt to production.")


def pass_deal(href, notes=""):
    """Soft-delete a listing (add to deleted_listings.txt + DB soft-delete)."""
    # Add to deleted file so it's filtered from UI and notifications
    deleted_file = DATA_DIR / "deleted_listings.txt"
    with open(deleted_file, "a") as f:
        f.write(href + "\n")

    # Remove from favorites if present
    fav_file = DATA_DIR / "favorite_listings.txt"
    if fav_file.exists():
        lines = [l.strip() for l in fav_file.read_text().splitlines() if l.strip()]
        if href in lines:
            lines.remove(href)
            fav_file.write_text("\n".join(lines) + "\n" if lines else "")
            print(f"  Removed from favorites")

    # Soft-delete in local DB
    db = Database()
    db.open()
    db.delete_listing(href)
    db.close()

    print(f"PASS: {href[:60]}... {notes}")

    # Soft-delete on production (no downtime)
    sql = (f"UPDATE listings SET deleted_at = CURRENT_TIMESTAMP "
           f"WHERE href = '{href}' AND deleted_at IS NULL")
    _apply_sql_on_prod(sql, "soft-delete")


def fetch_ksl_listing(url):
    """Fetch a KSL listing detail page via HTTP and extract description + specs.

    Bypasses PerimeterX bot detection by using plain HTTP with browser-like
    headers — same approach as the KSL scraper.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) "
            "Gecko/20100101 Firefox/128.0"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://cars.ksl.com/search",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })

    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    html = resp.text

    result = {"url": url, "raw_length": len(html)}

    # Extract title from <title> tag
    title_match = re.search(r"<title>(.*?)</title>", html)
    if title_match:
        result["title"] = title_match.group(1).replace(" | KSL Cars", "").strip()

    # Extract Next.js RSC data chunks for listing details
    pattern = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', re.DOTALL)
    descriptions = []
    for match in pattern.finditer(html):
        chunk = match.group(1)
        try:
            unescaped = chunk.encode("utf-8").decode("unicode_escape")
        except Exception:
            continue

        # Look for description text — usually contains vehicle details
        # KSL embeds the description in the RSC payload as a string
        if "description" in unescaped.lower() or "rebuilt" in unescaped.lower() \
                or "clean title" in unescaped.lower() or "branded" in unescaped.lower() \
                or "accident" in unescaped.lower() or "miles" in unescaped.lower():
            # Extract readable text segments (skip short/metadata chunks)
            for segment in re.findall(r'"([^"]{50,})"', unescaped):
                if any(kw in segment.lower() for kw in
                       ["miles", "vehicle", "condition", "title", "engine",
                        "transmission", "accident", "rebuilt", "branded",
                        "clean", "owner", "carfax", "contact", "call"]):
                    descriptions.append(segment)

    # Also try extracting from plain HTML (fallback)
    # KSL renders description in a <div> or <p> tag in the Description tab
    desc_html = re.findall(
        r'<(?:p|div)[^>]*class="[^"]*[Dd]escription[^"]*"[^>]*>(.*?)</(?:p|div)>',
        html, re.DOTALL
    )
    for d in desc_html:
        clean = re.sub(r"<[^>]+>", " ", d).strip()
        if len(clean) > 30:
            descriptions.append(clean)

    # Deduplicate and pick the longest description
    seen = set()
    unique_descs = []
    for d in descriptions:
        d_clean = d.strip()
        if d_clean not in seen and len(d_clean) > 30:
            seen.add(d_clean)
            unique_descs.append(d_clean)

    if unique_descs:
        # Sort by length, longest first (most likely the real description)
        unique_descs.sort(key=len, reverse=True)
        result["description"] = unique_descs[0]
        if len(unique_descs) > 1:
            result["other_text"] = unique_descs[1:5]

    # Extract structured data from meta tags
    for meta in re.finditer(r'<meta\s+(?:name|property)="([^"]+)"\s+content="([^"]*)"', html):
        name, content = meta.group(1), meta.group(2)
        if "title" in name.lower():
            result.setdefault("meta_title", content)
        elif "description" in name.lower():
            result.setdefault("meta_description", content)

    return result


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "sync":
        sync_from_prod()

    elif cmd == "pull":
        sync_from_prod()
        count = 20
        if "--count" in sys.argv:
            idx = sys.argv.index("--count")
            count = int(sys.argv[idx + 1])
        deals = pull_top_deals(count)
        print(json.dumps(deals, indent=2))

    elif cmd == "push":
        sync_to_prod()

    elif cmd == "fix":
        if len(sys.argv) < 3:
            print("Usage: analyze_deals.py fix <href> [--field value ...]")
            return
        href = sys.argv[2]
        kwargs = {}
        i = 3
        while i < len(sys.argv):
            if sys.argv[i] == "--no-sync":
                i += 1
                continue
            if sys.argv[i].startswith("--") and i + 1 < len(sys.argv):
                key = sys.argv[i][2:]
                val = sys.argv[i + 1]
                # Try numeric conversion
                try:
                    val = float(val) if "." in val else int(val)
                except ValueError:
                    pass
                kwargs[key] = val
                i += 2
            else:
                i += 1
        apply_fix(href, **kwargs)
        # SQL changes are already pushed live by apply_fix.
        # Only sync deleted_listings.txt if not --no-sync.
        if "--no-sync" not in sys.argv:
            sync_to_prod()

    elif cmd == "pass":
        if len(sys.argv) < 3:
            print("Usage: analyze_deals.py pass <href> [--notes 'reason']")
            return
        href = sys.argv[2]
        notes = ""
        if "--notes" in sys.argv:
            idx = sys.argv.index("--notes")
            notes = sys.argv[idx + 1]
        pass_deal(href, notes)
        # SQL soft-delete is already pushed live by pass_deal.
        # Only sync deleted_listings.txt if not --no-sync.
        if "--no-sync" not in sys.argv:
            sync_to_prod()

    elif cmd == "fav":
        if len(sys.argv) < 3:
            print("Usage: analyze_deals.py fav <href> [--notes 'reason']")
            return
        href = sys.argv[2]
        notes = ""
        if "--notes" in sys.argv:
            idx = sys.argv.index("--notes")
            notes = sys.argv[idx + 1]
        fav_deal(href, notes)

    elif cmd == "fetch":
        if len(sys.argv) < 3:
            print("Usage: analyze_deals.py fetch <ksl_url>")
            return
        url = sys.argv[2]
        if "ksl.com" not in url:
            print("ERROR: fetch currently only supports KSL URLs")
            return
        result = fetch_ksl_listing(url)
        print(json.dumps(result, indent=2))

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
