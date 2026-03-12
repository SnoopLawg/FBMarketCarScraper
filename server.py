"""Always-on entrypoint for Docker: load existing deals from DB and serve the web UI."""

import logging

from config import load_config
from database import Database
from analysis import clean_listings, calculate_averages, find_deals
from web_ui import start_web_ui

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


def main():
    config = load_config()
    db = Database()
    db.open()

    try:
        db.backfill_title_types()
        db.backfill_vins()
        deals = find_deals(db, config["DesiredCar"], config)
        logging.info(f"Loaded {len(deals)} deals from database.")
    except Exception as e:
        logging.warning(f"Could not load deals on startup: {e}")
        deals = []
    finally:
        db.close()

    start_web_ui(deals)


if __name__ == "__main__":
    main()
