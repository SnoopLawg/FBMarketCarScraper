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


# ── Owner count detection ────────────────────────────────────────

_OWNER_PATTERNS = [
    # "one owner", "1 owner", "1-owner", "single owner", "first owner"
    (re.compile(r"\b(?:one|1|single|first)[- ]?owner\b", re.I), 1),
    # "two owner", "2 owner", "second owner"
    (re.compile(r"\b(?:two|2|second)[- ]?owner\b", re.I), 2),
    # "three owner", "3 owner", "3+ owner", "multiple owner", "several owner"
    (re.compile(
        r"\b(?:three|3\+?|four|4|five|5|multiple|several)[- ]?owner", re.I), 3),
]


def parse_owner_count(text):
    """Detect owner count from listing description text.

    Returns:
        int or None: 1, 2, 3+ or None if not mentioned.
    """
    if not text:
        return None
    for pattern, count in _OWNER_PATTERNS:
        if pattern.search(text):
            return count
    return None


# ── Service history detection ────────────────────────────────────

_SERVICE_POSITIVE = re.compile(
    r"\b(?:"
    r"service\s+record|maintenance\s+record|service\s+history|"
    r"dealer\s+maintain|well[- ]maintain|regularly\s+maintain|"
    r"all\s+records|full\s+records|complete\s+records|"
    r"carfax\s+clean|clean\s+carfax|carfax\s+available|"
    r"autocheck|vehicle\s+history\s+report|"
    r"oil\s+change\s+record|documented\s+service|"
    r"always\s+serviced|garage\s+kept"
    r")",
    re.I,
)

_SERVICE_NEGATIVE = re.compile(
    r"\b(?:"
    r"no\s+(?:service|maintenance)\s+record|"
    r"no\s+records|missing\s+records|"
    r"unknown\s+(?:service|maintenance)\s+history"
    r")",
    re.I,
)


def parse_service_history(text):
    """Detect service history signals from listing description text.

    Returns:
        str: 'positive', 'negative', or None if not mentioned.
    """
    if not text:
        return None
    # Check negative patterns first (they're more specific)
    if _SERVICE_NEGATIVE.search(text):
        return "negative"
    if _SERVICE_POSITIVE.search(text):
        return "positive"
    return None
