"""Background scraper worker with status tracking."""

import logging
import threading
from datetime import datetime
from pathlib import Path

from config import load_config
from database import Database
from driver import create_driver
from scrapers import ALL_SCRAPERS
from analysis import clean_listings, calculate_averages, find_deals

_lock = threading.Lock()
_status = {
    "running": False,
    "phase": "idle",       # idle | starting | scraping | analyzing | done | error
    "source": "",
    "progress": 0,
    "message": "",
    "started_at": None,
    "finished_at": None,
    "error": None,
    "deal_count": 0,
}


def get_status():
    """Return a copy of the current scrape status."""
    return dict(_status)


def start_scrape(on_complete=None):
    """Start a background scrape. Returns (started: bool, message: str)."""
    if _status["running"]:
        return False, "A scrape is already in progress."

    if not _lock.acquire(blocking=False):
        return False, "Could not acquire lock. Try again."

    thread = threading.Thread(
        target=_run_scrape, args=(on_complete,), daemon=True
    )
    thread.start()
    return True, "Scrape started."


def start_enrich(on_complete=None, limit=100):
    """Start a background enrichment-only run (visits FB detail pages).

    This does NOT scrape new listings — it visits existing Facebook
    listings that are missing title_type and fills in title/accident/
    condition data from the detail pages.
    """
    if _status["running"]:
        return False, "A scrape or enrichment is already in progress."

    if not _lock.acquire(blocking=False):
        return False, "Could not acquire lock. Try again."

    thread = threading.Thread(
        target=_run_enrich, args=(on_complete, limit), daemon=True
    )
    thread.start()
    return True, f"Enrichment started (up to {limit} listings)."


def _run_enrich(on_complete, limit):
    """Background thread: visit FB detail pages to extract title types."""
    try:
        _status.update({
            "running": True,
            "phase": "scraping",
            "source": "facebook",
            "progress": 5,
            "message": "Starting title enrichment...",
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "error": None,
        })

        config = load_config()
        db = Database()
        db.open()

        # Count how many need enrichment
        rows_needed = db.get_listings_missing_title_type(source="facebook", limit=limit)
        total_needed = len(rows_needed)

        if total_needed == 0:
            _status.update({
                "phase": "done",
                "progress": 100,
                "message": "No listings need enrichment.",
                "finished_at": datetime.now().isoformat(),
                "running": False,
            })
            db.close()
            _lock.release()
            return

        _status.update({
            "progress": 10,
            "message": f"Creating browser for {total_needed} listings...",
        })

        driver = create_driver()

        try:
            from scrapers.facebook import FacebookScraper
            # We only need the scraper for enrichment — not for scraping
            fb = FacebookScraper(driver, config, lambda **kw: None)

            _status.update({
                "progress": 15,
                "message": f"Enriching {total_needed} listings...",
            })

            enriched = fb.enrich_listings(db, limit=limit)

            _status.update({
                "progress": 70,
                "message": f"Enriched {enriched}/{total_needed}. Running backfill...",
            })
        finally:
            driver.quit()

        # Backfill any remaining from car_name keywords
        db.backfill_title_types()

        # Re-run analysis so scores update
        _status.update({
            "phase": "analyzing",
            "progress": 80,
            "message": "Recalculating scores...",
        })

        desired_cars = config["DesiredCar"]
        mileage_threshold = config.get("MileageMax") or 150000
        clean_listings(db, desired_cars)
        calculate_averages(db, desired_cars, mileage_threshold)

        _status.update({"progress": 90, "message": "Finding deals..."})
        deals = find_deals(db, desired_cars, config)
        db.close()

        _status.update({
            "phase": "done",
            "progress": 100,
            "message": f"Enrichment complete! {enriched} titles updated, {len(deals)} deals.",
            "finished_at": datetime.now().isoformat(),
            "running": False,
            "deal_count": len(deals),
        })

        if on_complete:
            on_complete(deals)

    except Exception as e:
        logging.error(f"Background enrichment failed: {e}")
        _status.update({
            "phase": "error",
            "error": str(e),
            "running": False,
            "finished_at": datetime.now().isoformat(),
        })
    finally:
        if _lock.locked():
            _lock.release()


def _run_scrape(on_complete):
    """The actual scrape pipeline, runs in a background thread."""
    try:
        _status.update({
            "running": True,
            "phase": "starting",
            "source": "",
            "progress": 0,
            "message": "Initializing...",
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "error": None,
        })

        config = load_config()
        db = Database()
        db.open()

        sources = config.get("Sources", {})
        enabled = {k: v for k, v in sources.items() if v.get("enabled", True)}
        total_sources = len(enabled)

        if total_sources == 0:
            _status.update({
                "phase": "error",
                "error": "No sources enabled",
                "running": False,
            })
            db.close()
            return

        # Phase 1: Scrape
        _status.update({"phase": "scraping", "message": "Creating browser..."})
        driver = create_driver()

        deleted_file = Path(__file__).parent / "deleted_listings.txt"
        deleted_set = set()
        if deleted_file.exists():
            deleted_set = set(
                l.strip() for l in deleted_file.read_text().splitlines()
                if l.strip()
            )

        def insert_fn(**kwargs):
            db.insert_listing(**kwargs, deleted_set=deleted_set)

        fb_scraper = None
        try:
            for i, name in enumerate(enabled):
                scraper_cls = ALL_SCRAPERS.get(name)
                if not scraper_cls:
                    continue

                pct = int((i / total_sources) * 70)
                _status.update({
                    "source": name,
                    "progress": pct,
                    "message": f"Scraping {name} ({i+1}/{total_sources})...",
                })

                try:
                    scraper = scraper_cls(driver, config, insert_fn)
                    scraper.scrape()
                    # Keep reference to Facebook scraper for enrichment
                    if name == "facebook":
                        fb_scraper = scraper
                except Exception as e:
                    logging.error(f"{name} scraper failed: {e}")

            # Enrich Facebook listings with detail page data
            if fb_scraper:
                _status.update({
                    "progress": 72,
                    "message": "Enriching listings with detail page data...",
                })
                try:
                    fb_scraper.enrich_listings(db, limit=100)
                except Exception as e:
                    logging.error(f"Facebook enrichment failed: {e}")
        finally:
            driver.quit()

        # Mark stale
        for source in enabled:
            stale = db.mark_stale(source, days_old=7)
            if stale:
                logging.info(f"Marked {stale} stale {source} listings")

        # Phase 2: Analysis
        _status.update({
            "phase": "analyzing",
            "progress": 75,
            "message": "Backfilling title types...",
        })
        db.backfill_title_types()

        _status.update({"message": "Cleaning listings..."})
        desired_cars = config["DesiredCar"]
        mileage_threshold = config.get("MileageMax") or 150000
        clean_listings(db, desired_cars)

        _status.update({"progress": 85, "message": "Calculating averages..."})
        calculate_averages(db, desired_cars, mileage_threshold)

        _status.update({"progress": 90, "message": "Finding deals..."})
        deals = find_deals(db, desired_cars, config)

        db.close()

        _status.update({
            "phase": "done",
            "progress": 100,
            "message": f"Complete! Found {len(deals)} deals.",
            "finished_at": datetime.now().isoformat(),
            "running": False,
            "deal_count": len(deals),
        })

        if on_complete:
            on_complete(deals)

    except Exception as e:
        logging.error(f"Background scrape failed: {e}")
        _status.update({
            "phase": "error",
            "error": str(e),
            "running": False,
            "finished_at": datetime.now().isoformat(),
        })
    finally:
        _lock.release()
