"""Always-on entrypoint for Docker: load existing deals from DB and serve the web UI."""

import logging

from config import load_config, get_all_search_queries, load_discovery_cars
from database import Database
from analysis import clean_listings, calculate_averages, find_deals, find_sell_data
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
        db.backfill_owner_counts()
        db.backfill_vins()
        db.backfill_listed_at()

        all_cars = get_all_search_queries(config)
        mileage_threshold = config.get("MileageMax") or 150000
        clean_listings(db, all_cars)
        calculate_averages(db, all_cars, mileage_threshold)

        deals = find_deals(db, all_cars, config)
        sell_data = find_sell_data(db, config.get("SellCars", []), config)
        logging.info(f"Loaded {len(deals)} deals from database.")

        # Compute discovery deals
        discovery_deals = []
        disc_cars = load_discovery_cars(config)
        if disc_cars:
            disc_names = [c["name"] for c in disc_cars]
            with_data = [c for c in disc_names
                         if db.has_listings_for_query(c)]
            if with_data:
                clean_listings(db, with_data)
                calculate_averages(db, with_data, mileage_threshold)
                discovery_deals = find_deals(
                    db, with_data, config, is_discovery=True)
                logging.info(
                    f"Loaded {len(discovery_deals)} discovery deals.")
    except Exception as e:
        logging.warning(f"Could not load deals on startup: {e}")
        deals = []
        sell_data = []
        discovery_deals = []
    finally:
        db.close()

    start_web_ui(deals, sell_data=sell_data,
                 discovery_deals=discovery_deals)


if __name__ == "__main__":
    main()
