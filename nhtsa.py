"""NHTSA Vehicle Safety API client with SQLite caching.

Fetches safety ratings, complaint counts, and recall counts from the
free public NHTSA API (no key required).  Results are cached in
the database for 30 days to avoid hammering the API.
"""

import logging
import re
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import requests

# Common model-name fixups:  car_query token  ->  NHTSA model string
_MODEL_ALIASES = {
    "f150": "F-150",
    "f-150": "F-150",
    "f250": "F-250",
    "f-250": "F-250",
    "f350": "F-350",
    "rav4": "RAV4",
    "cr-v": "CR-V",
    "crv": "CR-V",
    "cx-5": "CX-5",
    "cx5": "CX-5",
    "cx-9": "CX-9",
    "cx-30": "CX-30",
    "cx-50": "CX-50",
    "hr-v": "HR-V",
    "4runner": "4Runner",
    "grand cherokee": "Grand Cherokee",
    "wrangler": "Wrangler",
    "outback": "Outback",
    "forester": "Forester",
    "crosstrek": "Crosstrek",
    "model 3": "Model 3",
    "model y": "Model Y",
}

_BASE = "https://api.nhtsa.gov"
_TIMEOUT = 15  # seconds per request
_CACHE_DAYS = 30


def parse_make_model(car_query):
    """Split a car_query like 'toyota tacoma' into (make, model).

    Returns (make, model) with NHTSA-friendly casing.
    """
    parts = car_query.strip().lower().split(None, 1)
    if len(parts) < 2:
        return parts[0] if parts else "", ""

    make = parts[0]
    model_raw = parts[1]

    # Check alias table first (handles multi-word models too)
    model = _MODEL_ALIASES.get(model_raw, model_raw)

    return make, model


def _get_json(url):
    """Fetch JSON from NHTSA. Returns parsed dict or None on failure."""
    try:
        resp = requests.get(url, timeout=_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.warning(f"[NHTSA] Request failed: {url} — {e}")
        return None


def fetch_safety_rating(make, model, year):
    """Fetch NHTSA safety rating for a vehicle.

    Returns dict with:
      overall_rating (int 1-5 or None),
      complaints_count (int),
      recalls_count (int),
      rollover_rating (int 1-5 or None),
      front_crash_rating (int 1-5 or None),
      side_crash_rating (int 1-5 or None),
    or None if vehicle not found.
    """
    # Step 1: get VehicleId(s)
    url1 = (
        f"{_BASE}/SafetyRatings/modelyear/{year}"
        f"/make/{quote(make)}/model/{quote(model)}"
    )
    data = _get_json(url1)
    if not data:
        return None

    results = data.get("Results") or data.get("results") or []
    if not results:
        # Try without hyphens / alternate spellings
        return None

    # Pick first variant (they share overall safety; differ only for
    # 2WD vs 4WD sub-ratings which we don't need granularity on).
    vehicle_id = results[0].get("VehicleId")
    if not vehicle_id:
        return None

    # Step 2: get actual ratings
    url2 = f"{_BASE}/SafetyRatings/VehicleId/{vehicle_id}"
    data2 = _get_json(url2)
    if not data2:
        return None

    results2 = data2.get("Results") or data2.get("results") or []
    if not results2:
        return None

    r = results2[0]

    def star(key):
        """Parse a star rating string like '4' -> int, 'Not Rated' -> None."""
        val = r.get(key, "Not Rated")
        if val and val != "Not Rated":
            try:
                return int(val)
            except (ValueError, TypeError):
                pass
        return None

    return {
        "overall_rating": star("OverallRating"),
        "front_crash_rating": star("OverallFrontCrashRating"),
        "side_crash_rating": star("OverallSideCrashRating"),
        "rollover_rating": star("RolloverRating"),
        "complaints_count": r.get("ComplaintsCount", 0) or 0,
        "recalls_count": r.get("RecallsCount", 0) or 0,
    }


def fetch_complaints_count(make, model, year):
    """Fetch complaint count from the NHTSA Complaints API.

    This is a fallback when the Safety Ratings endpoint doesn't include
    the complaint count (some vehicles are unrated but still have complaints).
    """
    url = (
        f"{_BASE}/complaints/complaintsByVehicle"
        f"?make={quote(make)}&model={quote(model)}&modelYear={year}"
    )
    data = _get_json(url)
    if not data:
        return 0
    # The complaints API uses lowercase keys
    return data.get("count", 0) or 0


def get_vehicle_rating(db, make, model, year):
    """Get vehicle rating — from cache if fresh, else from API.

    Parameters
    ----------
    db : Database
        An open Database instance (used for cache read/write).
    make, model : str
        Vehicle make and model (e.g. 'toyota', 'tacoma').
    year : int
        Model year.

    Returns
    -------
    dict or None
        Rating dict with overall_rating, complaints_count, etc.
    """
    make_l = make.strip().lower()
    model_l = model.strip().lower()

    # Check cache
    cached = db.get_vehicle_rating(make_l, model_l, year)
    if cached:
        fetched_str = cached.get("fetched_at") or cached.get("created_at", "")
        if fetched_str:
            try:
                fetched_dt = datetime.fromisoformat(fetched_str.replace("Z", "+00:00"))
                if datetime.utcnow() - fetched_dt < timedelta(days=_CACHE_DAYS):
                    return cached
            except (ValueError, TypeError):
                pass

    # Fetch from API
    logging.info(f"[NHTSA] Fetching rating for {year} {make} {model}...")
    rating = fetch_safety_rating(make, model, year)

    # If the Safety Ratings API had no data, try complaints API alone
    if rating is None:
        complaints = fetch_complaints_count(make, model, year)
        if complaints > 0:
            rating = {
                "overall_rating": None,
                "front_crash_rating": None,
                "side_crash_rating": None,
                "rollover_rating": None,
                "complaints_count": complaints,
                "recalls_count": 0,
            }

    if rating is None:
        # Store a "not found" marker so we don't re-fetch every run
        rating = {
            "overall_rating": None,
            "front_crash_rating": None,
            "side_crash_rating": None,
            "rollover_rating": None,
            "complaints_count": 0,
            "recalls_count": 0,
        }

    # Save to cache
    db.upsert_vehicle_rating(
        make=make_l, model=model_l, year=year,
        overall_rating=rating["overall_rating"],
        front_crash=rating.get("front_crash_rating"),
        side_crash=rating.get("side_crash_rating"),
        rollover=rating.get("rollover_rating"),
        complaints=rating["complaints_count"],
        recalls=rating["recalls_count"],
    )

    # Be polite — small delay between API calls during batch fetches
    time.sleep(0.5)

    return rating


def fetch_recalls(make, model, year):
    """Fetch recall details for a vehicle from NHTSA Recalls API.

    Returns list of dicts with:
      campaign_number, component, summary, consequence, remedy, report_date
    or empty list if none found / API fails.
    """
    url = (
        f"{_BASE}/recalls/recallsByVehicle"
        f"?make={quote(make)}&model={quote(model)}&modelYear={year}"
    )
    data = _get_json(url)
    if not data:
        return []

    results = data.get("results") or data.get("Results") or []
    recalls = []
    for r in results:
        recalls.append({
            "campaign_number": (r.get("NHTSACampaignNumber")
                                or r.get("campaignNumber") or ""),
            "component": r.get("Component") or r.get("component") or "",
            "summary": r.get("Summary") or r.get("summary") or "",
            "consequence": r.get("Consequence") or r.get("consequence") or "",
            "remedy": r.get("Remedy") or r.get("remedy") or "",
            "report_date": (r.get("ReportReceivedDate")
                            or r.get("reportReceivedDate") or ""),
        })
    return recalls


def get_vehicle_recalls_cached(db, make, model, year):
    """Get recall details — from cache if fresh, else from API.

    Returns list of recall dicts (may be empty if vehicle has no recalls).
    Returns None only on total failure.
    """
    make_l = make.strip().lower()
    model_l = model.strip().lower()

    # Check cache
    cached = db.get_vehicle_recalls(make_l, model_l, year)
    if cached is not None:
        # Check TTL on the first row
        fetched_str = cached[0].get("fetched_at", "") if cached else ""
        if fetched_str:
            try:
                fetched_dt = datetime.fromisoformat(
                    fetched_str.replace("Z", "+00:00"))
                if datetime.utcnow() - fetched_dt < timedelta(days=_CACHE_DAYS):
                    # Filter out sentinel rows
                    return [r for r in cached
                            if r["campaign_number"] != "__none__"]
            except (ValueError, TypeError):
                pass

    # Fetch from API
    logging.info(f"[NHTSA] Fetching recalls for {year} {make} {model}...")
    recalls = fetch_recalls(make, model, year)

    # Cache the results
    db.upsert_vehicle_recalls(make_l, model_l, year, recalls)

    time.sleep(0.3)
    return recalls


def get_recalls_batch(db, car_queries_with_years):
    """Fetch recalls for multiple (make, model, year) combos, using cache.

    Returns dict {(car_query_lower, year): [recall_dicts]}
    """
    results = {}
    for car_query, year in car_queries_with_years:
        make, model = parse_make_model(car_query)
        if not make or not model or not year:
            continue
        key = (car_query.lower(), year)
        if key not in results:
            recalls = get_vehicle_recalls_cached(db, make, model, year)
            if recalls is not None:
                results[key] = recalls
    return results


def get_ratings_batch(db, car_queries_with_years):
    """Fetch ratings for multiple (make, model, year) combos, using cache.

    Parameters
    ----------
    db : Database
        Open Database instance.
    car_queries_with_years : set of (car_query, year) tuples

    Returns
    -------
    dict  {(car_query_lower, year): rating_dict}
    """
    results = {}
    for car_query, year in car_queries_with_years:
        make, model = parse_make_model(car_query)
        if not make or not model or not year:
            continue
        key = (car_query.lower(), year)
        if key not in results:
            rating = get_vehicle_rating(db, make, model, year)
            if rating:
                results[key] = rating
    return results
