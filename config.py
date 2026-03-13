"""Configuration loading and validation."""

import json
import logging
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("DATA_DIR", SCRIPT_DIR))
CONFIG_PATH = DATA_DIR / "Config.json"


def load_config(path=None):
    """Load and return the config dict from Config.json."""
    config_path = path or CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(f"Config.json not found at {config_path}")
    with open(config_path, "r") as f:
        config = json.load(f)

    # Validate required fields
    required = ["DesiredCar", "MinPrice", "MaxPrice", "PriceThreshold"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")

    # Default Sources block if missing
    if "Sources" not in config:
        config["Sources"] = {
            "facebook": {"enabled": True, "CityID": config.get("CityID", "")},
        }

    # Default SellCars if missing
    if "SellCars" not in config:
        config["SellCars"] = []

    return config


def get_all_search_queries(config):
    """Return deduplicated union of buy + sell car names for scraping."""
    buy = list(config.get("DesiredCar", []))
    sell = [c["name"] for c in config.get("SellCars", []) if c.get("name")]
    seen = set()
    combined = []
    for name in buy + sell:
        key = name.lower().strip()
        if key not in seen:
            seen.add(key)
            combined.append(name)
    return combined


def save_config(config, path=None):
    """Write the config dict back to Config.json."""
    config_path = path or CONFIG_PATH
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    logging.info("Config saved.")


# ── Discovery Cars ──────────────────────────────────────────────

DISCOVERY_DEFAULTS_PATH = SCRIPT_DIR / "discovery_cars_default.json"

DISCOVERY_CATEGORIES = {
    "suv": "SUVs",
    "truck": "Trucks",
    "sedan": "Sedans",
    "hatch_wagon": "Hatchbacks & Wagons",
    "minivan": "Minivans",
    "sports": "Sports Cars",
}


def load_discovery_cars(config):
    """Return list of {name, category} dicts for discovery scraping.

    Uses DiscoveryCars overrides from config if present, else loads
    the built-in default list.  Filters out any car already in DesiredCar.
    """
    disc_config = config.get("DiscoveryCars", {})

    # Disabled entirely
    if disc_config is False or (isinstance(disc_config, dict)
                                and not disc_config.get("enabled", True)):
        return []

    # Load base list
    if isinstance(disc_config, dict) and "cars" in disc_config:
        cars = list(disc_config["cars"])
    else:
        try:
            with open(DISCOVERY_DEFAULTS_PATH, "r") as f:
                cars = json.load(f)
        except FileNotFoundError:
            logging.warning("discovery_cars_default.json not found")
            return []

    # Apply disabled_categories filter
    disabled_cats = set()
    if isinstance(disc_config, dict):
        disabled_cats = set(disc_config.get("disabled_categories", []))
    if disabled_cats:
        cars = [c for c in cars if c["category"] not in disabled_cats]

    # Apply removed_cars filter
    removed = set()
    if isinstance(disc_config, dict):
        removed = set(r.lower().strip()
                      for r in disc_config.get("removed_cars", []))
    if removed:
        cars = [c for c in cars if c["name"].lower().strip() not in removed]

    # Add custom_cars
    if isinstance(disc_config, dict):
        for custom in disc_config.get("custom_cars", []):
            if custom.get("name"):
                cars.append(custom)

    # Filter out anything already in DesiredCar (tracked cars)
    tracked = set(n.lower().strip() for n in config.get("DesiredCar", []))
    # Also filter out sell cars
    for sc in config.get("SellCars", []):
        if sc.get("name"):
            tracked.add(sc["name"].lower().strip())

    cars = [c for c in cars if c["name"].lower().strip() not in tracked]

    return cars


def get_discovery_batch(config, source, db):
    """Return car name strings for this discovery run.

    Facebook gets a smaller batch (12) due to aggressive anti-bot.
    Other sources get 30.  Wraps around when reaching end of list.
    Rotation state is persisted in DB so it survives restarts.
    """
    cars = load_discovery_cars(config)
    if not cars:
        return []

    disc_config = config.get("DiscoveryCars", {})
    if isinstance(disc_config, dict):
        fb_batch = disc_config.get("fb_batch_size", 12)
        other_batch = disc_config.get("batch_size", 30)
    else:
        fb_batch = 12
        other_batch = 30

    batch_size = fb_batch if source == "facebook" else other_batch
    total = len(cars)

    idx = db.get_rotation_index(source)
    # Build the batch, wrapping around
    batch = []
    for i in range(batch_size):
        pos = (idx + i) % total
        batch.append(cars[pos]["name"])

    # Update rotation index for next run
    new_idx = (idx + batch_size) % total
    db.update_rotation_index(source, new_idx)

    return batch


def get_discovery_category_map(config):
    """Return {car_name_lower: category_id} lookup dict."""
    cars = load_discovery_cars(config)
    return {c["name"].lower().strip(): c["category"] for c in cars}
