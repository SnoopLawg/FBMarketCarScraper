"""Configuration loading and validation."""

import json
import logging
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "Config.json"


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

    return config


def save_config(config, path=None):
    """Write the config dict back to Config.json."""
    config_path = path or CONFIG_PATH
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    logging.info("Config saved.")
