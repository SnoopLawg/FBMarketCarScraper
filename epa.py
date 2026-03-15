"""EPA Fuel Economy API client with SQLite caching.

Fetches city/highway/combined MPG data from the free fueleconomy.gov API
(no key required).  Results are cached in the vehicle_ratings table alongside
NHTSA data.

API docs: https://www.fueleconomy.gov/feg/ws/index.shtml
"""

import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from urllib.parse import quote

import requests

from nhtsa import parse_make_model, _MODEL_ALIASES

_BASE = "https://www.fueleconomy.gov/ws/rest"
_TIMEOUT = 15
_CACHE_DAYS = 30
_GAS_PRICE_PER_GAL = 3.50  # rough national average for fuel cost estimates

# Sentinel value stored in mpg_combined to mean "checked, no data from EPA"
_MPG_NOT_FOUND = -1

# EPA uses different model names than NHTSA in some cases.
# Map: lowercase token from car_query → EPA model name prefix to match against.
_EPA_MODEL_MAP = {
    "f150": "F150",
    "f-150": "F150",
    "f250": "F250",
    "f-250": "F250",
    "f350": "F350",
    "f-350": "F350",
    "rav4": "RAV4",
    "cr-v": "CR-V",
    "crv": "CR-V",
    "cx-5": "CX-5",
    "cx5": "CX-5",
    "cx-9": "CX-9",
    "cx-30": "CX-30",
    "cx-50": "CX-50",
    "hr-v": "HR-V",
    "hrv": "HR-V",
    "4runner": "4Runner",
    "mazda3": "3",
    "mazda6": "6",
    "model 3": "Model 3",
    "model y": "Model Y",
    "model s": "Model S",
    "model x": "Model X",
    "c-hr": "C-HR",
    "gr86": "GR86",
    "brz": "BRZ",
    "wrx": "WRX",
    "mx-5": "MX-5",
}


def _get_xml(url):
    """Fetch XML from fueleconomy.gov and parse it. Returns ElementTree root or None."""
    try:
        resp = requests.get(url, timeout=_TIMEOUT)
        resp.raise_for_status()
        return ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"[EPA] Request failed: {url} — {e}")
        return None


def _capitalize_make(make):
    """Capitalize make for EPA API (e.g. 'toyota' → 'Toyota')."""
    return make.strip().title()


def _epa_model_name(model):
    """Get the EPA-style model prefix to match against the model list.

    E.g. 'rav4' → 'RAV4', 'f150' → 'F150', 'tacoma' → 'Tacoma'
    """
    return _EPA_MODEL_MAP.get(model.lower(), model.strip().title())


def _strip_drivetrain(name):
    """Remove trailing drivetrain suffixes for comparison.

    'Tacoma 2WD' → 'Tacoma', 'CR-V AWD' → 'CR-V'
    """
    return re.sub(r'\s+(2WD|4WD|AWD|FWD|RWD)\b.*$', '', name).strip()


def _fetch_epa_model_list(make_cap, year):
    """Fetch the list of model names EPA knows about for a make/year.

    Returns list of model name strings, e.g. ['Tacoma 2WD', 'Tacoma 4WD', ...].
    """
    url = f"{_BASE}/vehicle/menu/model?year={year}&make={quote(make_cap)}"
    root = _get_xml(url)
    if root is None:
        return []
    models = []
    for item in root.findall(".//menuItem"):
        text = item.findtext("text")
        if text:
            models.append(text)
    return models


def _find_best_epa_model(target_model, epa_models):
    """Find the best matching EPA model name from the available model list.

    Strategy (in priority order):
      1. Exact match (case-insensitive)
      2. EPA model starts with our target (word boundary)
      3. Stripped (no drivetrain suffix) matches our target
      4. Pick the shortest / simplest variant (base model, usually 2WD)

    Returns the best EPA model name string, or None if no match.
    """
    if not epa_models:
        return None

    target_lower = target_model.lower().strip()

    # 1. Exact match
    for m in epa_models:
        if m.lower() == target_lower:
            return m

    # 2. Starts with our target model name
    candidates = []
    for m in epa_models:
        m_lower = m.lower()
        # Must match at word boundary: "Tacoma" matches "Tacoma 2WD"
        # but "Cam" should not match "Camry"
        if m_lower.startswith(target_lower + " ") or m_lower.startswith(target_lower + "/"):
            candidates.append(m)

    # 3. Try stripping drivetrain from EPA names and matching
    if not candidates:
        for m in epa_models:
            stripped = _strip_drivetrain(m).lower()
            if stripped == target_lower:
                candidates.append(m)

    if not candidates:
        return None

    # Among candidates, prefer the shortest name (most generic / base model).
    # Among equal length, prefer 2WD/FWD over AWD/4WD (usually better MPG,
    # and represents the "base" configuration — gives conservative estimate).
    def sort_key(m):
        ml = m.lower()
        drivetrain_penalty = 0
        if "4wd" in ml or "awd" in ml:
            drivetrain_penalty = 1
        return (len(m), drivetrain_penalty)

    candidates.sort(key=sort_key)
    return candidates[0]


def _get_vehicle_id(make_cap, model_epa, year):
    """Get EPA vehicle ID for a specific make/model/year.

    Returns the vehicle ID string or None.
    """
    url = (f"{_BASE}/vehicle/menu/options"
           f"?year={year}&make={quote(make_cap)}&model={quote(model_epa)}")
    root = _get_xml(url)
    if root is None:
        return None

    for item in root.findall(".//menuItem"):
        vid = item.findtext("value")
        if vid:
            return vid
    return None


def _get_vehicle_mpg(vehicle_id):
    """Fetch MPG data for a specific EPA vehicle ID.

    Returns dict {mpg_city, mpg_highway, mpg_combined} or None.
    """
    url = f"{_BASE}/vehicle/{vehicle_id}"
    vroot = _get_xml(url)
    if vroot is None:
        return None

    def int_val(tag):
        text = vroot.findtext(tag)
        if not text:
            return None
        try:
            return int(round(float(text)))
        except (ValueError, TypeError):
            return None

    city = int_val("city08")
    highway = int_val("highway08")
    combined = int_val("comb08")

    if not combined:
        return None

    return {
        "mpg_city": city,
        "mpg_highway": highway,
        "mpg_combined": combined,
    }


def fetch_mpg(make, model, year):
    """Fetch MPG data from EPA fueleconomy.gov.

    Strategy:
      1. Try direct model name in the options endpoint
      2. If no match, fetch EPA's model list for this make/year
      3. Fuzzy-match our model name against EPA's list
      4. Fetch the vehicle ID from matched model → get MPG

    Returns dict {mpg_city, mpg_highway, mpg_combined} or None.
    """
    make_cap = _capitalize_make(make)
    target = _epa_model_name(model)

    # Step 1: try direct lookup
    vehicle_id = _get_vehicle_id(make_cap, target, year)

    # Step 2: if direct lookup failed, fetch model list and fuzzy-match
    if not vehicle_id:
        epa_models = _fetch_epa_model_list(make_cap, year)
        if epa_models:
            best = _find_best_epa_model(target, epa_models)
            if best and best != target:
                logging.info(f"[EPA] Model fuzzy-match: '{target}' → '{best}'")
                vehicle_id = _get_vehicle_id(make_cap, best, year)

    if not vehicle_id:
        logging.info(f"[EPA] No vehicle options for {year} {make_cap} {target}")
        return None

    # Step 3: get MPG data for this vehicle
    return _get_vehicle_mpg(vehicle_id)


def get_mpg_cached(db, make, model, year):
    """Get MPG data — from vehicle_ratings cache if available, else from API.

    Returns dict {mpg_city, mpg_highway, mpg_combined} or None.
    """
    make_l = make.strip().lower()
    model_l = model.strip().lower()

    # Check if we already have MPG data cached in vehicle_ratings
    cached = db.get_vehicle_rating(make_l, model_l, year)
    if cached:
        mpg_val = cached.get("mpg_combined")
        if mpg_val is not None:
            if mpg_val == _MPG_NOT_FOUND:
                # Previously checked — EPA has no data for this vehicle
                return None
            if mpg_val > 0:
                return {
                    "mpg_city": cached["mpg_city"],
                    "mpg_highway": cached["mpg_highway"],
                    "mpg_combined": cached["mpg_combined"],
                }

    # Fetch from EPA API
    logging.info(f"[EPA] Fetching MPG for {year} {make} {model}...")
    mpg = fetch_mpg(make, model, year)

    if mpg:
        # Store in vehicle_ratings alongside NHTSA data
        db.update_vehicle_mpg(
            make_l, model_l, year,
            mpg["mpg_city"], mpg["mpg_highway"], mpg["mpg_combined"])
    else:
        # Store sentinel so we don't re-fetch every time
        db.update_vehicle_mpg(make_l, model_l, year,
                              _MPG_NOT_FOUND, _MPG_NOT_FOUND, _MPG_NOT_FOUND)

    time.sleep(0.3)
    return mpg


def get_mpg_batch(db, car_queries_with_years):
    """Fetch MPG for multiple (car_query, year) combos, using cache.

    Checks cache first (fast), then fetches uncached items in parallel.

    Returns dict {(car_query_lower, year): {mpg_city, mpg_highway, mpg_combined}}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = {}
    to_fetch = []  # [(key, make, model, year)]

    # Phase 1: check cache (serial, fast)
    for car_query, year in car_queries_with_years:
        make, model = parse_make_model(car_query)
        if not make or not model or not year:
            continue
        key = (car_query.lower(), year)
        if key in results:
            continue
        # Check cache inline
        make_l = make.strip().lower()
        model_l = model.strip().lower()
        cached = db.get_vehicle_rating(make_l, model_l, year)
        if cached:
            mpg_val = cached.get("mpg_combined")
            if mpg_val is not None:
                if mpg_val == _MPG_NOT_FOUND:
                    continue  # Previously checked — no data
                if mpg_val > 0:
                    results[key] = {
                        "mpg_city": cached["mpg_city"],
                        "mpg_highway": cached["mpg_highway"],
                        "mpg_combined": cached["mpg_combined"],
                    }
                    continue
        to_fetch.append((key, make, model, year))

    if not to_fetch:
        return results

    # Phase 2: fetch uncached in parallel
    logging.info(f"[EPA] Fetching MPG for {len(to_fetch)} car/year "
                 f"combos in parallel...")

    def _fetch_one(item):
        key, make, model, year = item
        logging.info(f"[EPA] Fetching MPG for {year} {make} {model}...")
        return key, make, model, year, fetch_mpg(make, model, year)

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_fetch_one, item) for item in to_fetch]
        for f in as_completed(futures):
            try:
                key, make, model, year, mpg = f.result()
                make_l = make.strip().lower()
                model_l = model.strip().lower()
                if mpg:
                    results[key] = mpg
                    db.update_vehicle_mpg(
                        make_l, model_l, year,
                        mpg["mpg_city"], mpg["mpg_highway"],
                        mpg["mpg_combined"])
                else:
                    db.update_vehicle_mpg(
                        make_l, model_l, year,
                        _MPG_NOT_FOUND, _MPG_NOT_FOUND, _MPG_NOT_FOUND)
            except Exception as e:
                logging.warning(f"[EPA] MPG fetch failed: {e}")

    return results


def estimate_monthly_fuel_cost(mpg_combined, miles_per_month=1000,
                                gas_price=_GAS_PRICE_PER_GAL):
    """Estimate monthly fuel cost given MPG and driving assumptions.

    Default: 1,000 miles/month at $3.50/gallon.
    """
    if not mpg_combined or mpg_combined <= 0:
        return None
    return round(miles_per_month / mpg_combined * gas_price)
