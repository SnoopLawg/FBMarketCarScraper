"""VIN cross-validation — compare VIN decode data against listing claims.

Detects mismatches between what a seller claims (year, make, drivetrain)
and what the VIN actually says.  Mismatches are scored by severity:
  - major:  year or make doesn't match (listing may be fraudulent)
  - minor:  drivetrain doesn't match (listing may be inaccurate)
"""

# Make aliases — handle common alternate spellings
_MAKE_ALIASES = {
    "chevy": "chevrolet",
    "vw": "volkswagen",
    "mercedes-benz": "mercedes",
    "mercedes benz": "mercedes",
    "land rover": "landrover",
}

# Drivetrain normalization mapping — handles both listing values and VIN values
_DT_NORMALIZE = {
    "all-wheel drive": "awd",
    "all wheel drive": "awd",
    "awd": "awd",
    "4wd": "4wd",
    "4x4": "4wd",
    "four wheel drive": "4wd",
    "4-wheel drive": "4wd",
    "front-wheel drive": "fwd",
    "front wheel drive": "fwd",
    "fwd": "fwd",
    "rear-wheel drive": "rwd",
    "rear wheel drive": "rwd",
    "rwd": "rwd",
    "2wd": "fwd",
    "4x2": "fwd",  # NHTSA VIN decode uses "4x2" for 2WD/FWD
}

# AWD/4WD are considered equivalent for validation purposes
_AWD_GROUP = {"awd", "4wd"}
# FWD/RWD/2WD are considered equivalent (both are "not AWD/4WD")
_2WD_GROUP = {"fwd", "rwd"}


def _normalize_make(make):
    """Normalize make name for comparison."""
    if not make:
        return ""
    m = make.strip().lower()
    return _MAKE_ALIASES.get(m, m)


def _normalize_drivetrain(dt):
    """Normalize drivetrain string for comparison.

    Handles compound VIN drive_type strings like '4WD/4-Wheel Drive/4x4'
    by splitting on '/' and checking each part.
    """
    if not dt:
        return ""
    dt_l = dt.strip().lower()

    # Direct lookup first
    if dt_l in _DT_NORMALIZE:
        return _DT_NORMALIZE[dt_l]

    # VIN decode often returns compound strings like "4WD/4-Wheel Drive/4x4"
    # Split on "/" and check each part
    for part in dt_l.split("/"):
        part = part.strip()
        if part in _DT_NORMALIZE:
            return _DT_NORMALIZE[part]

    return dt_l


def validate_vin_against_listing(vin_data, listing):
    """Compare VIN decode data against listing claims.

    Parameters
    ----------
    vin_data : dict
        VIN decode result with keys: year, make, model, drive_type, etc.
    listing : dict
        Deal/listing dict with keys: year, car_query, drivetrain, etc.

    Returns
    -------
    dict with:
        mismatches: list of {field, listing_value, vin_value, severity}
        severity: "none", "minor", or "major"
    """
    mismatches = []

    if not vin_data or not listing:
        return {"mismatches": [], "severity": "none"}

    # ── Year check (major) ────────────────────────────────────────
    vin_year = vin_data.get("year")
    listing_year = listing.get("year")
    if vin_year and listing_year and vin_year != listing_year:
        mismatches.append({
            "field": "year",
            "listing_value": str(listing_year),
            "vin_value": str(vin_year),
            "severity": "major",
        })

    # ── Make check (major) ────────────────────────────────────────
    vin_make = _normalize_make(vin_data.get("make"))
    # Extract make from car_query (first word)
    car_query = listing.get("car_query") or ""
    listing_make = _normalize_make(car_query.split()[0] if car_query else "")
    if vin_make and listing_make and vin_make != listing_make:
        mismatches.append({
            "field": "make",
            "listing_value": listing_make,
            "vin_value": vin_make,
            "severity": "major",
        })

    # ── Drivetrain check (minor) ──────────────────────────────────
    # Only validate drivetrain if the listing has an explicit drivetrain
    # (not a default/inferred value — those are just guesses)
    listing_dt_source = listing.get("drivetrain_source", "")
    vin_dt = _normalize_drivetrain(vin_data.get("drive_type"))
    listing_dt = _normalize_drivetrain(listing.get("drivetrain"))
    if vin_dt and listing_dt and listing_dt_source != "default":
        # AWD and 4WD are treated as equivalent
        vin_dt_group = "awd_group" if vin_dt in _AWD_GROUP else vin_dt
        listing_dt_group = "awd_group" if listing_dt in _AWD_GROUP else listing_dt
        # FWD and RWD are treated as equivalent (both are "not AWD/4WD")
        if vin_dt_group not in ("awd_group",) and vin_dt in _2WD_GROUP:
            vin_dt_group = "2wd_group"
        if listing_dt_group not in ("awd_group",) and listing_dt in _2WD_GROUP:
            listing_dt_group = "2wd_group"
        if vin_dt_group != listing_dt_group:
            mismatches.append({
                "field": "drivetrain",
                "listing_value": listing.get("drivetrain", ""),
                "vin_value": vin_data.get("drive_type", ""),
                "severity": "minor",
            })

    # Determine overall severity
    severity = "none"
    for m in mismatches:
        if m["severity"] == "major":
            severity = "major"
            break
        elif m["severity"] == "minor":
            severity = "minor"

    return {
        "mismatches": mismatches,
        "severity": severity,
    }


def compute_vin_penalty(mismatches):
    """Compute score penalty from VIN mismatches.

    Returns negative number (penalty to subtract from score):
      - Major mismatch (year/make): -10 each
      - Minor mismatch (drivetrain): -3 each
      - Total capped at -15
    """
    if not mismatches:
        return 0

    penalty = 0
    for m in mismatches:
        if m["severity"] == "major":
            penalty -= 10
        elif m["severity"] == "minor":
            penalty -= 3

    return max(-15, penalty)
