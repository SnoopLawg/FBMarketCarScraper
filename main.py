"""Entry point: scrape → analyze → browse deals."""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

from config import load_config
from database import Database
from analysis import clean_listings, calculate_averages, find_deals
from notifications import notify_scrape_complete
from web_ui import start_web_ui

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


def run_scrapers(config, db):
    """Start the browser, run each enabled scraper, then quit."""
    from driver import create_driver
    from scrapers import ALL_SCRAPERS

    sources = config.get("Sources", {})
    enabled = {k: v for k, v in sources.items() if v.get("enabled", True)}

    if not enabled:
        logging.warning("No sources enabled in Config.json")
        return

    driver = create_driver(proxy_config=config.get("Proxy"))
    deleted_file = DATA_DIR / "deleted_listings.txt"
    deleted_set = set()
    if deleted_file.exists():
        deleted_set = set(l.strip() for l in deleted_file.read_text().splitlines() if l.strip())

    def insert_fn(**kwargs):
        db.insert_listing(**kwargs, deleted_set=deleted_set)

    try:
        for name in enabled:
            scraper_cls = ALL_SCRAPERS.get(name)
            if not scraper_cls:
                logging.warning(f"Unknown source '{name}', skipping.")
                continue

            logging.info(f"=== Starting {name} scraper ===")
            run_start = datetime.now()
            run_id = db.insert_scrape_run(name, run_start.isoformat())

            try:
                scraper = scraper_cls(driver, config, insert_fn)
                scraper.scrape()

                duration = (datetime.now() - run_start).total_seconds()
                yield_count = scraper.listing_count
                db.update_scrape_run(run_id,
                    finished_at=datetime.now().isoformat(),
                    status="completed",
                    listings_found=yield_count,
                    duration_seconds=round(duration, 1))
                logging.info(
                    f"[{name}] Complete: {yield_count} listings in {duration:.1f}s")

                # Yield health check
                health = db.get_scrape_health()
                src_health = health.get(name)
                if (src_health and src_health["runs_count"] >= 3
                        and src_health["avg_yield"] > 0):
                    ratio = yield_count / src_health["avg_yield"]
                    if ratio < 0.2:
                        logging.warning(
                            f"[{name}] CRITICAL: Found only {yield_count} "
                            f"listings vs {src_health['avg_yield']:.0f} avg "
                            f"— scraper may be broken!")
                    elif ratio < 0.5:
                        logging.warning(
                            f"[{name}] WARNING: Found {yield_count} listings "
                            f"vs {src_health['avg_yield']:.0f} avg")

            except Exception as e:
                logging.error(f"{name} scraper failed: {e}")
                screenshot_path = None
                try:
                    scraper_obj = scraper_cls(driver, config, insert_fn)
                    screenshot_path = scraper_obj.capture_screenshot("crash")
                except Exception:
                    pass

                duration = (datetime.now() - run_start).total_seconds()
                db.update_scrape_run(run_id,
                    finished_at=datetime.now().isoformat(),
                    status="failed",
                    errors=1,
                    error_message=str(e)[:500],
                    screenshot_path=screenshot_path,
                    duration_seconds=round(duration, 1))
    finally:
        driver.quit()


def run_analysis(config, db):
    """Clean listings, compute averages, and find deals."""
    desired_cars = config["DesiredCar"]
    mileage_threshold = config.get("MileageMax") or 150000

    clean_listings(db, desired_cars)
    calculate_averages(db, desired_cars, mileage_threshold)
    return find_deals(db, desired_cars, config)


def main():
    config = load_config()
    db = Database()
    db.open()

    skip_scrape = "--ui-only" in sys.argv

    try:
        if not skip_scrape:
            run_scrapers(config, db)
            # Mark listings not seen in 7 days as stale (soft-delete)
            for source in config.get("Sources", {}):
                stale = db.mark_stale(source, days_old=7)
                if stale:
                    logging.info(f"Marked {stale} stale {source} listings as deleted")

        # Backfill title_type from car_name keywords (catches sellers
        # who put "salvage" / "clean title" etc. in the listing title)
        db.backfill_title_types()

        # Backfill VINs from existing description text
        db.backfill_vins()

        deals = run_analysis(config, db)
        logging.info(f"Found {len(deals)} deals total.")

        if not skip_scrape:
            try:
                notify_scrape_complete(config, deals)
            except Exception as e:
                logging.error(f"Discord notification failed: {e}")
    finally:
        db.close()

    if deals:
        start_web_ui(deals)
    else:
        logging.info("No deals found. Adjust config or run again later.")


if __name__ == "__main__":
    main()
