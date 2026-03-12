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
    # "one owner", "1 owner", "1-owner", "single owner", "first owner",
    # "1st owner", "1 owners", "one time owner", "1 previous owner",
    # Spanish: "un dueño", "1 dueño", "un propietario"
    (re.compile(
        r"\b(?:one|1|single|first|1st)[- ]?(?:time[- ]?)?(?:previous\s+)?owners?\b|"
        r"\b(?:un|1)\s*dueños?\b|"
        r"\b(?:un|1)\s*propietarios?\b",
        re.I), 1),
    # "two owner", "2 owner", "second owner", "2nd owner", "2 owners",
    # Spanish: "2 dueños", "dos dueños"
    (re.compile(
        r"\b(?:two|2|second|2nd)[- ]?(?:previous\s+)?owners?\b|"
        r"\b(?:dos|2)\s*dueños?\b|"
        r"\b(?:dos|2)\s*propietarios?\b",
        re.I), 2),
    # "three owner", "3 owner", "3+ owner", "3rd owner", "multiple owner",
    # "several owner", "3 owners", "the 3rd owner",
    # Spanish: "3 dueños", "tres dueños"
    (re.compile(
        r"\b(?:three|3\+?|3rd|four|4|4th|five|5|multiple|several)[- ]?owners?|"
        r"\b(?:tres|3\+?|cuatro|4|cinco|5|múltiples|varios)\s*dueños?",
        re.I), 3),
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
    # Maintenance records
    r"service\s+record|maintenance\s+record|service\s+history|"
    r"all\s+records|full\s+records|complete\s+records|"
    r"oil\s+change\s+record|documented\s+service|"
    r"\d+\s+service\s+records|"
    # Maintenance done
    r"dealer\s+maintain|dealer\s+serviced|"
    r"well[- ]maintain|regularly\s+maintain|meticulously\s+maintain|"
    r"always\s+serviced|garage\s+kept|"
    r"regular\s+oil\s+change|well\s+cared\s+for|"
    r"fully\s+serviced|recently\s+serviced|just\s+serviced|"
    r"all\s+maintenance|maintenance\s+done|"
    # Inspected
    r"(?:fully|carefully|professionally)\s+inspected|"
    r"passed\s+(?:safety|emissions?)|safety.{0,5}emissions?\s+passed|"
    # Carfax / history
    r"carfax\s+clean|clean\s+carfax|carfax\s+available|"
    r"carfax\s+report|free\s+carfax|\bcarfax\b|"
    r"autocheck|vehicle\s+history\s+report|"
    # Condition signals
    r"no\s+mechanical\s+issues"
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
