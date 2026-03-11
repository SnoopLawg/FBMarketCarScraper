"""Entry point: scrape → analyze → browse deals."""

import sys
import logging
from pathlib import Path

from config import load_config
from database import Database
from driver import create_driver
from analysis import clean_listings, calculate_averages, find_deals
from web_ui import start_web_ui
from scrapers import ALL_SCRAPERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


def run_scrapers(config, db):
    """Start the browser, run each enabled scraper, then quit."""
    sources = config.get("Sources", {})
    enabled = {k: v for k, v in sources.items() if v.get("enabled", True)}

    if not enabled:
        logging.warning("No sources enabled in Config.json")
        return

    driver = create_driver()
    deleted_file = Path(__file__).parent / "deleted_listings.txt"
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
            try:
                scraper = scraper_cls(driver, config, insert_fn)
                scraper.scrape()
            except Exception as e:
                logging.error(f"{name} scraper failed: {e}")
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
    finally:
        db.close()

    if deals:
        start_web_ui(deals)
    else:
        logging.info("No deals found. Adjust config or run again later.")


if __name__ == "__main__":
    main()
