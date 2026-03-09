"""Listing cleanup, average price calculation, and deal detection."""

import logging


def clean_listings(db, desired_cars):
    """Remove listings whose title doesn't match the car query they were filed under."""
    logging.info("Cleaning mismatched listings...")
    for car_query in desired_cars:
        rows = db.get_listings_for_query(car_query)
        for row in rows:
            row_id, car_name = row[0], row[1]
            if car_query.lower() not in car_name.lower():
                db.delete_by_id(row_id)
                logging.info(f"Removed mismatched listing id={row_id}: '{car_name}'")


def calculate_averages(db, desired_cars, mileage_threshold):
    """Compute average prices per car_query per year, split by mileage bucket."""
    logging.info("Calculating average prices...")
    for car_query in desired_cars:
        rows = db.get_priced_listings(car_query)

        year_data = {}
        for row in rows:
            price, mileage, year = row[0], row[1], row[2]
            year_data.setdefault(year, []).append((price, mileage or 0))

        for year, data in year_data.items():
            lower = [p for p, m in data if m <= mileage_threshold]
            higher = [p for p, m in data if m > mileage_threshold]

            avg_lower = round(sum(lower) / len(lower)) if lower else 0
            avg_higher = round(sum(higher) / len(higher)) if higher else 0

            db.upsert_average(car_query, year, avg_lower, avg_higher)


def find_deals(db, desired_cars, config):
    """Identify listings priced significantly below their year's average."""
    logging.info("Assessing deals...")
    deals = []
    mileage_threshold = config.get("MileageMax") or 150000
    price_threshold = config["PriceThreshold"]
    location_filter = config.get("LocationFilter", "")
    min_mileage = config.get("MileageMin")
    max_mileage = config.get("MileageMax")

    for car_query in desired_cars:
        avg_by_year = db.get_averages(car_query)
        candidates = db.get_deal_candidates(car_query)

        for row in candidates:
            href = row["href"]
            price = row["price"]
            mileage = row["mileage"]
            year = row["year"]
            location = row["location"]
            source = row["source"]
            image_url = row["image_url"]
            car_name = row["car_name"]
            created_at = row["created_at"]
            updated_at = row["updated_at"]

            if not year or year not in avg_by_year:
                continue
            if location_filter and location_filter not in (location or ""):
                continue

            mileage = mileage or 0
            avg_lower, avg_higher = avg_by_year[year]
            is_deal = False

            if mileage <= mileage_threshold:
                if avg_lower > 0 and price < (avg_lower - price_threshold):
                    if min_mileage is None or mileage > min_mileage:
                        is_deal = True
            else:
                if avg_higher > 0 and price < (avg_higher - price_threshold):
                    if max_mileage is None or mileage < max_mileage:
                        is_deal = True

            if is_deal:
                deals.append({
                    "href": href,
                    "price": price,
                    "mileage": mileage,
                    "year": year,
                    "location": location,
                    "source": source,
                    "car_query": car_query,
                    "avg_price": avg_lower if mileage <= mileage_threshold else avg_higher,
                    "image_url": image_url,
                    "car_name": car_name,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "trim": row["trim"],
                    "seller": row["seller"],
                    "condition": row["condition"],
                    "deal_rating": row["deal_rating"],
                    "accident_history": row["accident_history"],
                    "distance": row["distance"],
                })

    logging.info(f"Found {len(deals)} deals.")
    return deals
