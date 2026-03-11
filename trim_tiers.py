"""Trim-level extraction and tiering for deal scoring.

Tiers:
  1 = Base      (S, SR, LX, XL, L, SE, Sport, Base)
  2 = Mid       (SR5, EX, XLT, SEL, SV, LE, XLE)
  3 = High      (TRD Sport, TRD Off-Road, EX-L, Lariat, Limited, XSE, Touring)
  4 = Premium   (TRD Pro, Platinum, King Ranch, Denali, Titanium, Calligraphy)
"""

import re

# ── Trim-to-tier mapping ─────────────────────────────────────────
# Keys are lowercased trim tokens.  More specific patterns first.

_TIER_4_PREMIUM = {
    "trd pro", "trail edition", "platinum", "king ranch",
    "denali", "titanium", "calligraphy", "prestige",
    "gt", "gt-line", "type r", "type s", "nismo", "shelby",
    "raptor", "tremor", "wildtrak", "first edition",
    "pinnacle", "reserve", "signature", "ultra",
    "premium plus", "black edition",
}

_TIER_3_HIGH = {
    "trd sport", "trd off-road", "trd off road", "trd offroad",
    "ex-l", "exl", "ex-l navi", "touring",
    "lariat", "limited", "xse", "adventure",
    "sel premium", "nightshade", "special edition",
    "trail", "trailhawk", "overland",
    "awd", "4x4",  # drivetrain upgrade
    "premium", "sport s",
    "xle premium",
}

_TIER_2_MID = {
    "sr5", "ex", "xlt", "sel", "sv", "le", "xle",
    "se awd", "sport", "active", "preferred",
    "latitude", "big bend", "outer banks",
    "sl", "slt", "lt", "ls", "ltz",
    "hybrid", "plug-in hybrid", "phev",
    "energi",
}

_TIER_1_BASE = {
    "s", "sr", "lx", "xl", "l", "se", "base", "ce",
    "dx", "dl", "nx", "st", "work truck", "wt",
    "access cab", "regular cab",
}


def extract_trim(car_name, car_query):
    """Extract the trim string from a car_name by removing the car_query portion.

    Examples:
      car_name="2020 Toyota Tacoma TRD Off-Road", car_query="toyota tacoma"
      → "TRD Off-Road"

      car_name="2018 Honda cr-v EX-L Sport Utility 4D", car_query="honda cr-v"
      → "EX-L"
    """
    if not car_name:
        return ""

    name_lower = car_name.lower()
    query_lower = car_query.lower()

    # Remove year (4 digits at start)
    name_clean = re.sub(r'^\d{4}\s+', '', car_name, flags=re.IGNORECASE)

    # Remove the car make+model (query)
    for word in query_lower.split():
        name_clean = re.sub(re.escape(word), '', name_clean, count=1, flags=re.IGNORECASE)

    # Remove common suffixes that aren't trim info
    suffixes = [
        r'\bsedan\s*\d*d?\b', r'\bcoupe\s*\d*d?\b', r'\bhatchback\s*\d*d?\b',
        r'\bsuv\b', r'\bsport utility\s*\d*d?\b', r'\bpickup\s*\d*d?\b',
        r'\bcrew cab\b', r'\bdouble cab\b', r'\baccess cab\b', r'\bregular cab\b',
        r'\bextended cab\b', r'\bquad cab\b', r'\bking cab\b',
        r'\b\d+\s*ft\b', r'\b\d+\.\d+l?\b', r'\bv[46]\b', r'\bi[46]\b',
        r'\b4wd\b', r'\b2wd\b', r'\bfwd\b', r'\brwd\b',
        r'\b\(natl\)\b', r'\bnatl\b',
        r'\bawd\b',
    ]
    for pat in suffixes:
        name_clean = re.sub(pat, '', name_clean, flags=re.IGNORECASE)

    # Clean up whitespace and punctuation
    name_clean = re.sub(r'["\']', '', name_clean)
    name_clean = re.sub(r'\s+', ' ', name_clean).strip()
    name_clean = name_clean.strip('- ,.')

    return name_clean


def get_trim_tier(car_name, car_query, trim_column=""):
    """Determine the trim tier (1-4) from available data.

    Checks trim_column first (structured data from scrapers),
    then falls back to extracting from car_name.

    Returns (tier: int 1-4, trim_label: str)
    """
    # Combine all available trim info
    extracted = extract_trim(car_name, car_query)
    sources = [s for s in [trim_column or "", extracted] if s.strip()]

    if not sources:
        return 1, ""  # No trim info → assume base

    # Check each source against tier maps, highest tier wins
    best_tier = 1
    best_label = sources[0] if sources else ""

    for src in sources:
        src_lower = src.lower().strip()

        # Check premium first (most specific)
        for pattern in _TIER_4_PREMIUM:
            if pattern in src_lower:
                if best_tier < 4:
                    best_tier = 4
                    best_label = src
                break

        # Check high
        for pattern in _TIER_3_HIGH:
            if pattern in src_lower:
                if best_tier < 3:
                    best_tier = 3
                    best_label = src
                break

        # Check mid
        for pattern in _TIER_2_MID:
            if pattern in src_lower:
                if best_tier < 2:
                    best_tier = 2
                    best_label = src
                break

        # Tier 1 is the default, but check for explicit base trims
        for pattern in _TIER_1_BASE:
            if pattern == src_lower or f" {pattern}" in f" {src_lower}":
                if best_label == src and best_tier == 1:
                    best_label = src
                break

    return best_tier, best_label


_TIER_NAMES = {1: "Base", 2: "Mid", 3: "High", 4: "Premium"}


def tier_name(tier):
    """Human-readable tier name."""
    return _TIER_NAMES.get(tier, "Base")
