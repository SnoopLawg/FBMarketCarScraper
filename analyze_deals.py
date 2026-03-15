#!/usr/bin/env python3
"""Pull top deals for AI analysis and apply corrections from findings.

Usage:
    # Sync production DB, then pull top 20 deals as JSON
    python analyze_deals.py pull [--count 20]

    # Apply corrections from analysis findings (syncs back to prod)
    python analyze_deals.py fix <href> [--price 13000] [--title_type rebuilt] [--year 2019] [--notes "reason"]

    # Soft-delete a listing (verdict: PASS) — removes from UI + notifications
    python analyze_deals.py pass <href> [--notes "rebuilt title, overpriced"]

    # Just sync the DB from production (no analysis)
    python analyze_deals.py sync
"""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

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


def sync_to_prod():
    """Push the local DB and deleted_listings.txt back to production."""
    prod_data_dir = str(Path(PROD_DB_PATH).parent)
    files = [
        (str(LOCAL_DB), f"{PROD_HOST}:{PROD_DB_PATH}"),
    ]
    # Also sync deleted_listings.txt if it exists
    deleted_file = DATA_DIR / "deleted_listings.txt"
    if deleted_file.exists():
        files.append(
            (str(deleted_file), f"{PROD_HOST}:{prod_data_dir}/deleted_listings.txt"))

    for local, remote in files:
        name = Path(local).name
        result = subprocess.run(
            ["scp", local, remote],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"ERROR: scp {name} failed: {result.stderr.strip()}")
        else:
            print(f"Pushed {name} to production.")


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
    print(f"Updated {href[:60]}... "
          f"fields={list(direct_fields.keys()) + list(detail_fields.keys())} "
          f"notes={notes}")

    db.close()


def pass_deal(href, notes=""):
    """Soft-delete a listing (add to deleted_listings.txt + DB soft-delete)."""
    # Add to deleted file so it's filtered from UI and notifications
    deleted_file = DATA_DIR / "deleted_listings.txt"
    with open(deleted_file, "a") as f:
        f.write(href + "\n")

    # Soft-delete in DB
    db = Database()
    db.open()
    db.delete_listing(href)
    db.close()

    print(f"PASS: {href[:60]}... {notes}")


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

    elif cmd == "fix":
        if len(sys.argv) < 3:
            print("Usage: analyze_deals.py fix <href> [--field value ...]")
            return
        href = sys.argv[2]
        kwargs = {}
        i = 3
        while i < len(sys.argv):
            if sys.argv[i].startswith("--"):
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
        sync_to_prod()

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
