"""Shared parsing helpers for price, mileage, and year extraction."""

import re


def parse_price(price_str):
    """Parse a price string like '$3,500' into a float."""
    if not price_str or price_str == "Sold":
        return None
    try:
        cleaned = price_str.replace("$", "").replace(",", "").strip()
        return float(cleaned)
    except ValueError:
        return None


def parse_mileage(mileage_str):
    """Parse a mileage string like '120K miles' into a float (absolute miles)."""
    if not mileage_str or mileage_str == "N/A":
        return None
    match = re.search(r"(\d+[\d,]*\.?\d*)\s*[Kk]?", mileage_str)
    if not match:
        return None
    try:
        num = float(match.group(1).replace(",", ""))
        if num < 1000:
            num *= 1000
        return num
    except ValueError:
        return None


def extract_year(car_name):
    """Extract a 4-digit year (19xx or 20xx) from a car title."""
    match = re.search(r"\b(19|20)\d{2}\b", car_name)
    return int(match.group()) if match else None
