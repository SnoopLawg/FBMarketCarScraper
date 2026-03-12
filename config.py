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
