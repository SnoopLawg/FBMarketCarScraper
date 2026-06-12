"""Shared parsing helpers for price, mileage, and year extraction."""

import re
from datetime import datetime, timedelta


# ── Title-status detection ───────────────────────────────────────
# Specific, title-bearing phrases ONLY. Bare keywords ("salvage", "lemon")
# false-positived badly: Cars.com's "Lemon Law" disclaimer boilerplate and
# FB's related-listing rails produced 76 bogus "lemon" flags (vs 34 real
# salvage+rebuilt), each hard-capping a clean car to an F. Detection should
# be run on text already scoped to the listing's own content.
_TITLE_PHRASES = [
    # (matched phrase, resulting title_type) — order: most severe first
    ("salvage title", "salvage"),
    ("salvaged title", "salvage"),
    ("flood title", "salvage"),
    ("dismantled title", "salvage"),
    ("rebuilt title", "rebuilt"),
    ("rebuilt / branded", "rebuilt"),
    ("rebuilt/branded", "rebuilt"),
    ("branded title", "rebuilt"),
    ("reconstructed title", "rebuilt"),
    ("rebuilt/restored", "rebuilt"),
    ("r/r title", "rebuilt"),
    ("lemon law buyback", "lemon"),
    ("lemon law vehicle", "lemon"),
    ("manufacturer buyback", "lemon"),
    ("lemon title", "lemon"),
    ("clean title", "clean"),
]

# Reversed phrasing the fixed phrases miss, e.g. AutoSavvy's template
# "the current status of the title is branded". Requires 'title' adjacent to
# the status word (via 'is'/':'/'='), so bare 'branded'/'clean' in prose
# can't false-positive.
_TITLE_STATUS_RE = re.compile(
    r"\btitle\b\s*(?:status)?\s*(?:is|:|=)\s*(?:an?\s+|currently\s+)?"
    r"(branded|rebuilt|reconstructed|salvaged?|flood|dismantled|clean)\b")
_STATUS_MAP = {
    "branded": "rebuilt", "rebuilt": "rebuilt", "reconstructed": "rebuilt",
    "salvage": "salvage", "salvaged": "salvage", "flood": "salvage",
    "dismantled": "salvage", "clean": "clean",
}
_SEVERITY = {"salvage": 0, "rebuilt": 1, "lemon": 2, "clean": 3}


def detect_title_type(text):
    """Detect title status from listing text, or None if not stated.

    Trusts only specific title-bearing phrases — never bare "salvage"/
    "lemon"/"rebuilt", which match boilerplate and unrelated text. Returns
    one of 'salvage' | 'rebuilt' | 'lemon' | 'clean', or None.
    """
    t = (text or "").lower()
    for phrase, ttype in _TITLE_PHRASES:
        if phrase in t:
            return ttype
    found = [_STATUS_MAP[m] for m in _TITLE_STATUS_RE.findall(t)]
    if found:
        return min(found, key=lambda x: _SEVERITY[x])
    return None


# ── Powertrain detection ─────────────────────────────────────────
# Hybrids/PHEVs/EVs price on a different curve than the gas version of the
# same model/year (often $3-8k higher), so comps must not mix powertrains.

_EV_MODELS = re.compile(
    r"\b(model [3sxy]|leaf|bolt|ioniq ?5|ioniq ?6|mach-?e|id\.?4|ariya|"
    r"ev6|ev9|lightning|rivian|r1[ts]|lucid|polestar|i[34x]|e-?tron|"
    r"taycan|kona electric|niro ev)\b")
_HYBRID_MODELS = re.compile(r"\b(prius|insight|ioniq blue|niro(?! ev))\b")
_PHEV_RE = re.compile(r"\b(plug-?in|phev|prime|4xe)\b")
_HYBRID_RE = re.compile(r"\b(hybrid|hev)\b")
_EV_RE = re.compile(r"\b(electric|bev|ev)\b")


def detect_powertrain(name, trim="", vin_fuel=""):
    """Classify powertrain: 'phev' | 'hybrid' | 'ev' | '' (gas/unknown).

    Checks name+trim text and model knowledge; a VIN-decoded fuel type of
    Electric is authoritative for EVs. Conservative: bare 'electric' is only
    trusted in the name/trim (where it means the drivetrain), never in
    descriptions ('electric blue paint' would false-positive).
    """
    text = f"{name or ''} {trim or ''}".lower()
    if (vin_fuel or "").strip().lower() == "electric":
        return "ev"
    if _PHEV_RE.search(text):
        return "phev"
    if _HYBRID_RE.search(text) or _HYBRID_MODELS.search(text):
        return "hybrid"
    if _EV_MODELS.search(text) or _EV_RE.search(text):
        return "ev"
    return ""


# ── Seller type classification ───────────────────────────────────

_DEALER_KEYWORDS = re.compile(
    r"\b(?:LLC|Inc|Motors|Dealership|Auto\s*Group|Automotive|Pre[- ]?Owned|"
    r"Auto\s*Sales|Car\s*Sales|Used\s*Cars|BHPH|Buy\s*Here)\b",
    re.I,
)


def classify_seller_type(seller_name=None, href=None, source=None,
                         description=None):
    """Classify a listing as 'dealer', 'private', or None (unknown).

    Uses source-specific signals: seller name keywords
    for Cars.com/Autotrader, and description text for Facebook.
    """
    src = (source or "").lower()

    # Cars.com is a dealer-inventory marketplace — effectively 100% dealers
    # (private sellers can't list there). Default dealer.
    if src == "carscom":
        return "dealer"

    # Autotrader is dealer-dominated; its dealer names often lack obvious
    # keywords ("Ken Garff Hyundai"), so a name-keyword miss shouldn't demote
    # to private. Default dealer.
    if src == "autotrader":
        return "dealer"

    # Facebook Marketplace is predominantly PRIVATE sellers; flag dealer only
    # on an explicit signal (keyword in name or a dealer phrase in the post).
    if src == "facebook":
        if seller_name and _DEALER_KEYWORDS.search(seller_name):
            return "dealer"
        if description:
            d = description.lower()
            if "professional seller" in d or "dealership" in d:
                return "dealer"
        return "private"

    # KSL sets seller_type explicitly from its sellerType field (in ksl.py).
    return None


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


# ── Listing age detection ─────────────────────────────────────────

_LISTED_AGO = re.compile(
    r"Listed\s+(\d+)\s+(hour|day|week|month)s?\s+ago",
    re.I,
)


def parse_listed_date(text, reference_date=None):
    """Parse 'Listed N days/weeks/hours ago' into an ISO date string.

    FB Marketplace shows this on detail pages. The text is captured
    in the full-page description scrape, split across newlines, so
    callers should replace newlines with spaces before passing.

    Args:
        text: Listing text (newlines should be replaced with spaces).
        reference_date: When the text was scraped. Defaults to now.

    Returns:
        str: ISO date (e.g. '2026-02-05') or None if not found.
    """
    if not text:
        return None
    m = _LISTED_AGO.search(text)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2).lower()
    ref = reference_date or datetime.now()
    if unit == "hour":
        dt = ref - timedelta(hours=n)
    elif unit == "day":
        dt = ref - timedelta(days=n)
    elif unit == "week":
        dt = ref - timedelta(weeks=n)
    elif unit == "month":
        dt = ref - timedelta(days=n * 30)
    else:
        return None
    return dt.strftime("%Y-%m-%d")
