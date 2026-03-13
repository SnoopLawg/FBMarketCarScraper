"""Entry point: scrape → analyze → browse deals."""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

from config import load_config, get_all_search_queries, load_discovery_cars, get_discovery_batch
from database import Database
from analysis import clean_listings, calculate_averages, find_deals, find_sell_data
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

    # Discovery scrape phase
    disc_cars = load_discovery_cars(config)
    if disc_cars:
        logging.info("=== Starting discovery scrape phase ===")
        disc_driver = create_driver(proxy_config=config.get("Proxy"))

        def disc_insert_fn(**kwargs):
            db.insert_listing(**kwargs, deleted_set=deleted_set,
                             is_discovery=True)

        try:
            for name in enabled:
                scraper_cls = ALL_SCRAPERS.get(name)
                if not scraper_cls:
                    continue
                batch = get_discovery_batch(config, name, db)
                if not batch:
                    continue
                logging.info(
                    f"[discovery:{name}] Scraping {len(batch)} car models")
                try:
                    scraper = scraper_cls(disc_driver, config,
                                         disc_insert_fn, car_list=batch)
                    scraper.scrape()
                    logging.info(
                        f"[discovery:{name}] Found "
                        f"{scraper.listing_count} listings")
                except Exception as e:
                    logging.error(f"[discovery:{name}] Failed: {e}")
        finally:
            disc_driver.quit()


def run_analysis(config, db):
    """Clean listings, compute averages, and find deals."""
    all_cars = get_all_search_queries(config)
    mileage_threshold = config.get("MileageMax") or 150000

    clean_listings(db, all_cars)
    calculate_averages(db, all_cars, mileage_threshold)
    return find_deals(db, all_cars, config)


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

        # Backfill listed_at from FB "Listed N days ago" text
        db.backfill_listed_at()

        deals = run_analysis(config, db)
        logging.info(f"Found {len(deals)} deals total.")

        # Compute sell pricing recommendations
        sell_data = find_sell_data(db, config.get("SellCars", []), config)

        # Compute discovery deals
        discovery_deals = []
        disc_cars = load_discovery_cars(config)
        if disc_cars:
            disc_names = [c["name"] for c in disc_cars]
            with_data = [c for c in disc_names
                         if db.has_listings_for_query(c)]
            if with_data:
                mileage_threshold = config.get("MileageMax") or 150000
                clean_listings(db, with_data)
                calculate_averages(db, with_data, mileage_threshold)
                discovery_deals = find_deals(
                    db, with_data, config, is_discovery=True)
                logging.info(
                    f"Found {len(discovery_deals)} discovery deals.")

        if not skip_scrape:
            try:
                notify_scrape_complete(config, deals)
            except Exception as e:
                logging.error(f"Discord notification failed: {e}")
    finally:
        db.close()

    if deals or sell_data or discovery_deals:
        start_web_ui(deals, sell_data=sell_data,
                     discovery_deals=discovery_deals)
    else:
        logging.info("No deals found. Adjust config or run again later.")


if __name__ == "__main__":
    main()
