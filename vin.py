"""VIN extraction and NHTSA VIN decoding.

Extracts Vehicle Identification Numbers (VINs) from listing text and
decodes them via the free NHTSA vPIC API to get verified vehicle specs
(make, model, year, trim, body, drivetrain, fuel type).

No API key required — NHTSA vPIC is fully public.
"""

import logging
import re
import time

import requests

# ── VIN regex ────────────────────────────────────────────────────
# A valid VIN is exactly 17 characters, alphanumeric, excluding I, O, Q.
# We use word boundaries to avoid matching random 17-char strings.
_VIN_RE = re.compile(
    r'\b([A-HJ-NPR-Z0-9]{17})\b',
    re.IGNORECASE,
)

# Characters that are NOT valid in a VIN (used to filter false positives)
_INVALID_VIN_CHARS = set("IOQioq")

_DECODE_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValues/{vin}?format=json"
_TIMEOUT = 15


def extract_vin(text):
    """Extract a VIN from free-form text.

    Returns the first valid-looking VIN found, or None.
    VINs must be exactly 17 characters, using only A-Z 0-9 excluding I, O, Q.
    """
    if not text:
        return None

    matches = _VIN_RE.findall(text)
    for candidate in matches:
        candidate = candidate.upper()
        # Filter out strings that are all digits (likely not a VIN)
        if candidate.isdigit():
            continue
        # Filter out strings that are all letters (likely not a VIN)
        if candidate.isalpha():
            continue
        # Must have at least 1 letter and 1 digit (real VINs always do)
        has_letter = any(c.isalpha() for c in candidate)
        has_digit = any(c.isdigit() for c in candidate)
        if has_letter and has_digit:
            return candidate

    return None


def decode_vin(vin):
    """Decode a VIN using the NHTSA vPIC API.

    Returns a dict with vehicle details, or None on failure.
    Fields returned:
        make, model, year, trim, body_class, drive_type,
        fuel_type, engine, displacement, cylinders,
        plant_city, plant_country, error_code
    """
    vin = vin.upper().strip()
    url = _DECODE_URL.format(vin=vin)

    try:
        resp = requests.get(url, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.warning(f"[VIN] NHTSA decode failed for {vin}: {e}")
        return None

    results = data.get("Results")
    if not results:
        return None

    r = results[0]

    def val(key):
        """Get a value, returning None for empty/placeholder strings."""
        v = r.get(key, "")
        if not v or v in ("Not Applicable", ""):
            return None
        return v.strip()

    # Error code 0 = OK, 1 = some info missing, 5+ = bad VIN
    error_code = val("ErrorCode") or "0"
    # ErrorCode can be "0" or "1 - ...", etc.
    first_code = error_code.split(",")[0].split("-")[0].strip()
    try:
        error_num = int(first_code)
    except ValueError:
        error_num = 99

    if error_num >= 5:
        logging.info(f"[VIN] Invalid VIN {vin}: error code {error_code}")
        return None

    year_raw = val("ModelYear")
    try:
        year = int(year_raw) if year_raw else None
    except (ValueError, TypeError):
        year = None

    return {
        "vin": vin,
        "make": val("Make"),
        "model": val("Model"),
        "year": year,
        "trim": val("Trim"),
        "body_class": val("BodyClass"),
        "drive_type": val("DriveType"),
        "fuel_type": val("FuelTypePrimary"),
        "engine": val("EngineCylinders"),
        "displacement": val("DisplacementL"),
        "cylinders": val("EngineCylinders"),
        "plant_city": val("PlantCity"),
        "plant_country": val("PlantCountry"),
        "error_code": error_code,
    }


def decode_vin_cached(db, vin):
    """Decode a VIN, using the database cache when available.

    Parameters
    ----------
    db : Database
        An open Database instance (uses vin_cache table).
    vin : str
        The 17-character VIN to decode.

    Returns
    -------
    dict or None
    """
    vin = vin.upper().strip()

    # Check cache first
    cached = db.get_vin_data(vin)
    if cached is not None:
        return cached

    # Fetch from API
    result = decode_vin(vin)

    # Cache the result (even None → store as empty to avoid re-fetching)
    if result:
        db.upsert_vin_data(vin, result)
    else:
        # Store a "not found" marker
        db.upsert_vin_data(vin, {
            "vin": vin, "make": None, "model": None, "year": None,
            "trim": None, "body_class": None, "drive_type": None,
            "fuel_type": None, "engine": None, "displacement": None,
            "cylinders": None, "plant_city": None, "plant_country": None,
            "error_code": "not_found",
        })

    # Be polite to the free API
    time.sleep(0.3)

    return result
