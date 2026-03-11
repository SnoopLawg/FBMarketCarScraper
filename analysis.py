"""Listing cleanup, average price calculation, and deal scoring."""

import logging
from collections import defaultdict
from datetime import datetime

from nhtsa import parse_make_model, get_vehicle_rating, get_recalls_batch
from epa import get_mpg_batch, estimate_monthly_fuel_cost
from trim_tiers import get_trim_tier, tier_name
from drivetrain import detect_drivetrain, drivetrain_label, is_awd_or_4wd
from vin_validate import validate_vin_against_listing, compute_vin_penalty


def title_group(title_type):
    """Map title_type to an averaging group.

    Clean + unknown are grouped together (most unknown are likely clean).
    Rebuilt, salvage, and lemon each get their own group so that their
    averages reflect real market value for that title status.
    """
    t = (title_type or "").lower()
    if t in ("salvage", "rebuilt", "lemon"):
        return t
    return "clean"  # clean + unknown/null → same group


def clean_listings(db, desired_cars):
    """Remove listings whose title doesn't match the car query they were filed under."""
    logging.info("Cleaning mismatched listings...")
    for car_query in desired_cars:
        rows = db.get_listings_for_query(car_query)
        for row in rows:
            row_id, car_name = row[0], row[1]
            if car_query.lower() not in car_name.lower():
                db.delete_by_id(row_id)
                logging.info(f"Removed mismatched listing id={row_id}: '{car_name}'")


def calculate_averages(db, desired_cars, mileage_threshold):
    """Compute average prices per car_query per year, split by mileage bucket
    AND title group.

    Averages are computed separately for each title group (clean, rebuilt,
    salvage, lemon) so that a rebuilt car is compared against other rebuilt
    cars — not inflated by clean-title prices.  An "all" group is also
    stored as a fallback for groups with insufficient data (< 3 listings).
    """
    logging.info("Calculating average prices (title-group aware)...")
    for car_query in desired_cars:
        rows = db.get_priced_listings(car_query)

        # Group by (year, title_group)
        year_title_data = {}  # (year, title_group) → [(price, mileage)]
        year_all_data = {}    # year → [(price, mileage)]

        for row in rows:
            price, mileage, year = row[0], row[1], row[2]
            tt = row[3] if len(row) > 3 else None
            group = title_group(tt)

            year_title_data.setdefault((year, group), []).append(
                (price, mileage or 0))
            year_all_data.setdefault(year, []).append(
                (price, mileage or 0))

        # Per-group averages (only when enough data for a meaningful avg)
        for (year, group), data in year_title_data.items():
            if len(data) >= 3:
                lower = [p for p, m in data if m <= mileage_threshold]
                higher = [p for p, m in data if m > mileage_threshold]
                avg_lower = round(sum(lower) / len(lower)) if lower else 0
                avg_higher = round(sum(higher) / len(higher)) if higher else 0
                db.upsert_average(car_query, year, avg_lower, avg_higher,
                                  title_group=group)

        # "all" averages (fallback when a title group is too small)
        for year, data in year_all_data.items():
            lower = [p for p, m in data if m <= mileage_threshold]
            higher = [p for p, m in data if m > mileage_threshold]
            avg_lower = round(sum(lower) / len(lower)) if lower else 0
            avg_higher = round(sum(higher) / len(higher)) if higher else 0
            db.upsert_average(car_query, year, avg_lower, avg_higher,
                              title_group="all")


# ── Trim-Aware Averages ──────────────────────────────────────────

def _build_trim_averages(candidates, car_query, mileage_threshold,
                         target_group="clean"):
    """Compute per-tier average prices from candidate rows.

    Only includes listings from the same title_group so that trim-tier
    averages aren't skewed by mixing clean and branded titles.

    Returns dict: {(year, tier): avg_price}
    Falls back to all-tier average when a tier has < 3 listings.
    """
    # Bucket by (year, tier)
    buckets = defaultdict(list)      # (year, tier) → [prices]
    year_buckets = defaultdict(list)  # year → [prices]

    for row in candidates:
        price = row["price"]
        year = row["year"]
        if not year or not price:
            continue

        # Only include listings from the same title group
        row_group = title_group(row["title_type"])
        if row_group != target_group:
            continue

        tier, _ = get_trim_tier(row["car_name"], car_query, row["trim"] or "")
        buckets[(year, tier)].append(price)
        year_buckets[year].append(price)

    result = {}
    for (year, tier), prices in buckets.items():
        if len(prices) >= 3:
            result[(year, tier)] = round(sum(prices) / len(prices))
        else:
            # Fallback to all-tier average for this year
            all_prices = year_buckets.get(year, [])
            if all_prices:
                result[(year, tier)] = round(sum(all_prices) / len(all_prices))

    return result


# ── Deal Scoring ─────────────────────────────────────────────────

_EXPECTED_LIFESPAN = {
    # Miles a well-maintained example commonly reaches.
    # Sources: iSeeCars, Consumer Reports, mechanic consensus.
    "toyota": 300_000,
    "lexus": 300_000,
    "honda": 250_000,
    "acura": 250_000,
    "subaru": 220_000,
    "mazda": 220_000,
    "hyundai": 200_000,
    "kia": 200_000,
    "ford": 200_000,   # trucks/SUVs can go longer, but avg across lineup
    "chevrolet": 200_000,
    "chevy": 200_000,
    "gmc": 200_000,
    "ram": 200_000,
    "dodge": 180_000,
    "jeep": 180_000,
    "nissan": 180_000,
    "volkswagen": 170_000,
    "vw": 170_000,
    "bmw": 170_000,
    "mercedes": 170_000,
    "audi": 170_000,
    "volvo": 180_000,
    "buick": 190_000,
    "chrysler": 170_000,
    "mitsubishi": 180_000,
    "tesla": 250_000,
}
_DEFAULT_LIFESPAN = 200_000


def compute_deal_score(price, avg_price, mileage, year, deal_rating,
                       accident_history, title_type, nhtsa_rating,
                       trim_tier=1, trim_avg_price=None,
                       drivetrain="unknown", dt_source="unknown",
                       days_listed=0, car_query="",
                       vin_mismatches=None):
    """Compute a composite deal score from 0-100 with 7 factors.

    Title & Condition is the DOMINANT factor (25 pts) because a bad
    title (salvage/rebuilt) tanks a car's real value 30-60%.  Hard score
    caps ensure salvage cars can *never* grade well regardless of price.

    Factors:
      - Price vs Average: 30pts
      - Title & Condition: 25pts  ← THE biggest factor
      - Mileage: 15pts (age-relative — actual vs 12k mi/yr expected)
      - Reliability: 10pts (NHTSA safety + complaints)
      - Drivetrain: 10pts (AWD/4WD bonus — huge in snowy states)
      - Trim Value: 5pts (higher trim at same/lower price)
      - Freshness: 5pts (newer listings = fresh finds)

    Score caps by title type:
      - Clean / Unknown: no cap (0-100)
      - Rebuilt: capped at 45
      - Salvage: capped at 30
      - Lemon: capped at 25
    """
    price_score = 0.0
    mileage_score = 0.0
    reliability_score = 0.0
    drivetrain_score = 0.0
    condition_score = 0.0
    trim_score = 0.0
    freshness_score = 0.0

    # Reasoning explanations for each factor
    reasons = {}

    # Use trim-specific average if available, fall back to overall
    effective_avg = trim_avg_price if trim_avg_price and trim_avg_price > 0 else avg_price

    # ── Price factor (30 points max) ──────────────────────────────
    # Uses a square-root curve so moderate discounts score well.
    # 10% below avg → ~19pts, 20% below → ~27pts, 25%+ → maxes at 30.
    if effective_avg and effective_avg > 0 and price < effective_avg:
        price_ratio = (effective_avg - price) / effective_avg
        price_score = round(min(30.0, price_ratio ** 0.5 * 60.0), 1)
        pct = round(price_ratio * 100)
        avg_label = "trim avg" if trim_avg_price and trim_avg_price > 0 else "avg"
        reasons["price"] = (f"${price:,.0f} is {pct}% below "
                            f"${effective_avg:,.0f} {avg_label}")
    elif effective_avg and effective_avg > 0:
        pct = round((price - effective_avg) / effective_avg * 100)
        reasons["price"] = (f"${price:,.0f} is {pct}% above "
                            f"${effective_avg:,.0f} avg — no price credit")
    else:
        reasons["price"] = "No average price data to compare"

    # ── Title & Condition factor (25 points max) ──────────────────
    # This is the BIGGEST factor.  A salvage title is a dealbreaker;
    # a clean title is the baseline expectation for a real deal.
    #
    # Title base:  clean=18, unknown=4, rebuilt/salvage/lemon=0
    # Accidents:   no accident=+5, unknown=+2.5, reported=0
    # Deal rating: great=+2, good=+1.5, unknown=+1, fair=+0.5
    #
    # Max: 18 + 5 + 2 = 25

    title_lower = (title_type or "").lower()

    # Title base points (clean is the gold standard)
    if title_lower == "clean":
        title_pts = 18.0
    elif not title_lower or title_lower == "unknown":
        # Unknown = risky.  You wouldn't buy without checking.
        # Heavily penalized vs clean to avoid inflated scores.
        title_pts = 4.0
    else:
        # Salvage, rebuilt, lemon — no title credit
        title_pts = 0.0

    # Accident history — neutral default when data is missing
    accident_lower = (accident_history or "").lower()
    if "no accident" in accident_lower:
        accident_pts = 5.0
    elif "accident reported" in accident_lower or "1 accident" in accident_lower:
        accident_pts = 0.0
    else:
        # Unknown — give benefit of the doubt (most cars are clean)
        accident_pts = 2.5

    # Deal rating from marketplace — neutral default when missing
    rating_pts = 1.0  # baseline when marketplace hasn't rated it
    if deal_rating:
        rating = deal_rating.lower()
        if "great" in rating:
            rating_pts = 2.0
        elif "good" in rating:
            rating_pts = 1.5
        elif "fair" in rating:
            rating_pts = 0.5

    condition_score = round(min(25.0, title_pts + accident_pts + rating_pts), 1)

    # Build condition reasoning
    _cond_parts = []
    if title_lower == "clean":
        _cond_parts.append("Clean title (+18)")
    elif not title_lower or title_lower == "unknown":
        _cond_parts.append("Unknown title (+4)")
    else:
        _cond_parts.append(f"{title_type or 'Bad'} title (+0)")
    if "no accident" in accident_lower:
        _cond_parts.append("no accidents (+5)")
    elif "accident reported" in accident_lower or "1 accident" in accident_lower:
        _cond_parts.append("accident reported (+0)")
    else:
        _cond_parts.append("unknown accident history (+2.5)")
    if deal_rating:
        _cond_parts.append(f"{deal_rating} rating (+{rating_pts})")
    else:
        _cond_parts.append("no marketplace rating (+1)")
    reasons["condition"] = ", ".join(_cond_parts)

    # ── Mileage factor (15 points max) — age-relative + lifespan ───
    # Two layers:
    #   1) Age-relative: is this car driven more/less than 12k mi/yr?
    #   2) Lifespan: how much of this make's expected life is used up?
    # A Tacoma at 200k (67% of 300k lifespan) should score better than
    # a Nissan at 200k (111% of 180k lifespan).
    if mileage is not None and mileage > 0 and year:
        from datetime import date
        car_age_months = max(1, (date.today().year - year) * 12
                            + date.today().month)
        expected_miles = car_age_months * 1000          # 12k/yr baseline
        age_ratio = mileage / expected_miles            # <1 = low mi, >1 = high

        # Age-relative base score (0-15)
        if age_ratio <= 0.50:
            mileage_score = 15.0
        elif age_ratio <= 0.75:
            mileage_score = round(15.0 - (age_ratio - 0.50) / 0.25 * 2.0, 1)
        elif age_ratio <= 1.00:
            mileage_score = round(13.0 - (age_ratio - 0.75) / 0.25 * 2.0, 1)
        elif age_ratio <= 1.25:
            mileage_score = round(11.0 - (age_ratio - 1.00) / 0.25 * 3.0, 1)
        elif age_ratio <= 1.50:
            mileage_score = round(8.0 - (age_ratio - 1.25) / 0.25 * 4.0, 1)
        elif age_ratio <= 2.00:
            mileage_score = round(4.0 - (age_ratio - 1.50) / 0.50 * 4.0, 1)
        else:
            mileage_score = 0.0

        # Lifespan adjustment: bonus/penalty based on % of expected life used.
        # A Toyota at 60% life = bonus; a VW at 95% life = penalty.
        make = car_query.split()[0].lower() if car_query else ""
        lifespan = _EXPECTED_LIFESPAN.get(make, _DEFAULT_LIFESPAN)
        life_used = mileage / lifespan
        if life_used <= 0.40:
            mileage_score += 2.0            # plenty of life left
        elif life_used <= 0.60:
            mileage_score += 1.0            # solid
        elif life_used <= 0.80:
            pass                            # neutral
        elif life_used <= 1.00:
            mileage_score -= 1.5            # nearing end of expected life
        else:
            mileage_score -= 3.0            # past expected lifespan

        mileage_score = round(max(0.0, min(15.0, mileage_score)), 1)

        # Build mileage reasoning
        pct_of_expected = round(age_ratio * 100)
        pct_life = round(life_used * 100)
        if age_ratio <= 1.0:
            direction = f"{100 - pct_of_expected}% below"
        else:
            direction = f"{pct_of_expected - 100}% above"
        reasons["mileage"] = (f"{mileage:,.0f} mi is {direction} the "
                              f"{expected_miles:,.0f} mi expected for a {year} "
                              f"({pct_life}% of {make.title() or 'avg'} "
                              f"{lifespan:,} mi lifespan)")

    # ── Reliability factor (10 points max) — NHTSA data ───────────
    if nhtsa_rating:
        stars = nhtsa_rating.get("overall_rating")
        if stars is not None:
            star_points = {5: 5.0, 4: 4.0, 3: 3.0, 2: 1.5, 1: 0.5}
            reliability_score += star_points.get(stars, 2.5)
        else:
            reliability_score += 2.5

        # Complaint thresholds — popular cars naturally have more raw
        # complaints, so thresholds are generous to avoid penalizing
        # high-volume models like Civics and Tacomas unfairly.
        complaints = nhtsa_rating.get("complaints_count", 0)
        if complaints < 100:
            reliability_score += 5.0
        elif complaints < 250:
            reliability_score += 4.0
        elif complaints < 500:
            reliability_score += 2.5
        elif complaints < 800:
            reliability_score += 1.0
    else:
        reliability_score = 5.0

    reliability_score = round(min(10.0, reliability_score), 1)

    # Build reliability reasoning
    if nhtsa_rating:
        _stars = nhtsa_rating.get("overall_rating")
        _comp = nhtsa_rating.get("complaints_count", 0)
        _r_parts = []
        if _stars is not None:
            _r_parts.append(f"NHTSA {_stars}/5 stars")
        else:
            _r_parts.append("no NHTSA rating")
        _r_parts.append(f"{_comp:,} complaints")
        reasons["reliability"] = ", ".join(_r_parts)
    else:
        reasons["reliability"] = "No NHTSA data available — neutral score"

    # ── Drivetrain factor (10 points max) ─────────────────────────
    if is_awd_or_4wd(drivetrain):
        if dt_source == "explicit":
            drivetrain_score = 10.0
        else:
            drivetrain_score = 6.0
    elif drivetrain in ("fwd", "2wd"):
        if dt_source == "explicit":
            drivetrain_score = 2.0
        else:
            drivetrain_score = 3.0
    elif drivetrain == "rwd":
        drivetrain_score = 4.0
    else:
        drivetrain_score = 3.0

    # Build drivetrain reasoning
    dt_display = (drivetrain or "unknown").upper()
    if is_awd_or_4wd(drivetrain):
        _conf = "confirmed" if dt_source == "explicit" else "inferred"
        reasons["drivetrain"] = f"{dt_display} ({_conf})"
    elif drivetrain in ("fwd", "2wd"):
        reasons["drivetrain"] = f"{dt_display} — no AWD/4WD bonus"
    elif drivetrain == "rwd":
        reasons["drivetrain"] = "RWD — partial credit"
    else:
        reasons["drivetrain"] = "Unknown drivetrain — neutral score"

    # ── Trim Value factor (5 points max) ──────────────────────────
    if trim_tier >= 2 and avg_price and avg_price > 0:
        overall_discount = (avg_price - price) / avg_price if price < avg_price else 0
        tier_mult = {2: 0.5, 3: 0.75, 4: 1.0}.get(trim_tier, 0)
        trim_score = round(min(5.0, overall_discount * 20.0 * tier_mult), 1)
    trim_score = max(0.0, trim_score)

    # Build trim reasoning
    if trim_tier >= 2 and trim_score > 0:
        tier_names = {2: "Mid", 3: "High", 4: "Premium"}
        reasons["trim"] = f"{tier_names.get(trim_tier, 'Higher')} trim at a discount"
    elif trim_tier >= 2:
        reasons["trim"] = "Higher trim but not priced below avg"
    else:
        reasons["trim"] = "Base/standard trim"

    # ── Freshness factor (5 points max) ──────────────────────────
    if days_listed <= 1:
        freshness_score = 5.0
    elif days_listed <= 3:
        freshness_score = 4.0
    elif days_listed <= 7:
        freshness_score = 3.0
    elif days_listed <= 14:
        freshness_score = 2.0
    elif days_listed <= 30:
        freshness_score = 1.0
    else:
        freshness_score = 0.0

    # Build freshness reasoning
    if days_listed <= 1:
        reasons["freshness"] = "Listed today — fresh find"
    elif days_listed <= 3:
        reasons["freshness"] = f"Listed {days_listed} days ago"
    elif days_listed <= 7:
        reasons["freshness"] = f"Listed {days_listed} days ago — still recent"
    elif days_listed <= 30:
        reasons["freshness"] = f"Listed {days_listed} days ago — getting stale"
    else:
        reasons["freshness"] = f"Listed {days_listed} days ago — old listing"

    # ── Raw total ─────────────────────────────────────────────────
    raw_total = round(price_score + condition_score + mileage_score
                      + reliability_score + drivetrain_score + trim_score
                      + freshness_score, 1)

    # ── VIN cross-validation penalty ──────────────────────────────
    vin_penalty = 0
    if vin_mismatches:
        vin_penalty = compute_vin_penalty(vin_mismatches)
        raw_total = max(0, round(raw_total + vin_penalty, 1))
        _mm_parts = []
        for mm in vin_mismatches:
            _mm_parts.append(
                f"{mm['field']}: listing says {mm['listing_value']}, "
                f"VIN says {mm['vin_value']} ({mm['severity']})")
        reasons["vin_validation"] = (
            f"VIN mismatch ({vin_penalty:+d} penalty): "
            + "; ".join(_mm_parts))
    elif vin_mismatches is not None:
        # Empty list = we checked and everything matched
        reasons["vin_validation"] = "VIN verified — matches listing"
    else:
        reasons["vin_validation"] = "No VIN data to validate"

    # ── Hard score cap for bad titles ─────────────────────────────
    # Salvage/rebuilt/lemon can NEVER score well.  This is the key
    # insight: a "great price" on a salvage car is not a great deal.
    title_cap = 100.0
    if title_lower == "salvage":
        title_cap = 30.0
    elif title_lower == "rebuilt":
        title_cap = 45.0
    elif title_lower == "lemon":
        title_cap = 25.0

    total = round(min(raw_total, title_cap), 1)

    return {
        "total": total,
        "price_score": round(price_score, 1),
        "mileage_score": round(mileage_score, 1),
        "reliability_score": round(reliability_score, 1),
        "drivetrain_score": round(drivetrain_score, 1),
        "condition_score": round(condition_score, 1),
        "trim_score": round(trim_score, 1),
        "freshness_score": round(freshness_score, 1),
        "vin_penalty": vin_penalty,
        "reasons": reasons,
    }


def score_to_grade(score):
    """Convert numeric score to letter grade."""
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    if score >= 35:
        return "D"
    return "F"


def compute_market_range(prices):
    """Compute percentile-based market value ranges from a list of prices.

    Returns a dict with:
        count: number of data points
        low: 10th percentile (great deal territory)
        fair: 25th percentile (below average)
        avg: 50th percentile (median / fair market)
        high: 75th percentile (above average)
        premium: 90th percentile (overpriced)
    or None if insufficient data (< 5 listings).
    """
    if not prices or len(prices) < 5:
        return None

    prices = sorted(prices)
    n = len(prices)

    def percentile(p):
        """Linear interpolation percentile."""
        k = (n - 1) * p / 100.0
        f = int(k)
        c = f + 1
        if c >= n:
            return round(prices[-1])
        return round(prices[f] + (k - f) * (prices[c] - prices[f]))

    return {
        "count": n,
        "low": percentile(10),
        "fair": percentile(25),
        "avg": percentile(50),
        "high": percentile(75),
        "premium": percentile(90),
    }


def _title_cap(title_type):
    """Score cap based on title type.  Used by template for UI warning."""
    t = (title_type or "").lower()
    if t == "salvage":
        return 30
    if t == "rebuilt":
        return 45
    if t == "lemon":
        return 25
    return 100


# ── Deduplication ────────────────────────────────────────────────

def _dedup_deals(deals):
    """Remove duplicate listings (same car posted under multiple URLs).

    Groups by (car_name_lower, year, price).  Keeps the listing with
    the most filled-in fields (best data quality).
    """
    groups = defaultdict(list)
    for d in deals:
        key = (
            (d.get("car_name") or "").lower().strip(),
            d.get("year"),
            d.get("price"),
        )
        groups[key].append(d)

    deduped = []
    for key, group in groups.items():
        if len(group) == 1:
            deduped.append(group[0])
        else:
            # Keep the one with the most data
            def richness(d):
                """Count how many optional fields are filled."""
                fields = ["trim", "seller", "deal_rating", "accident_history",
                          "distance", "title_type", "image_url"]
                return sum(1 for f in fields if d.get(f))
            group.sort(key=richness, reverse=True)
            deduped.append(group[0])

    return deduped


# ── Main entry point ─────────────────────────────────────────────

def find_deals(db, desired_cars, config):
    """Score all candidates and return those above the minimum threshold."""
    logging.info("Assessing deals...")
    deals = []
    mileage_threshold = config.get("MileageMax") or 150000
    location_filter = config.get("LocationFilter", "")
    min_score = config.get("MinDealScore", 20)

    # Pre-fetch all NHTSA ratings for unique (car_query, year) combos
    all_candidates = {}
    car_year_combos = set()
    for car_query in desired_cars:
        candidates = db.get_deal_candidates(car_query)
        all_candidates[car_query] = candidates
        for row in candidates:
            if row["year"]:
                car_year_combos.add((car_query, row["year"]))

    # Fetch NHTSA ratings (uses cache, only hits API for uncached)
    nhtsa_cache = {}
    for car_query, year in car_year_combos:
        make, model = parse_make_model(car_query)
        if make and model:
            key = (car_query.lower(), year)
            if key not in nhtsa_cache:
                try:
                    rating = get_vehicle_rating(db, make, model, year)
                    if rating:
                        nhtsa_cache[key] = rating
                except Exception as e:
                    logging.warning(f"[NHTSA] Failed for {car_query} {year}: {e}")

    # Fetch NHTSA recall details (batch, cached)
    recalls_cache = {}
    try:
        recalls_cache = get_recalls_batch(db, car_year_combos)
        logging.info(f"Loaded recalls for {len(recalls_cache)} car/year combos.")
    except Exception as e:
        logging.warning(f"[NHTSA] Recalls batch fetch failed: {e}")

    # Fetch EPA MPG data (batch, cached)
    mpg_cache = {}
    try:
        mpg_cache = get_mpg_batch(db, car_year_combos)
        logging.info(f"Loaded MPG data for {len(mpg_cache)} car/year combos.")
    except Exception as e:
        logging.warning(f"[EPA] MPG batch fetch failed: {e}")

    # Pre-fetch VIN decode data for all candidates with VINs.
    # Uses NHTSA's batch decode endpoint for efficiency (up to 50 VINs per call).
    all_vins = set()
    for car_query in desired_cars:
        for row in all_candidates[car_query]:
            if row["vin"]:
                all_vins.add(row["vin"])
    if all_vins:
        from vin import decode_vins_batch_cached
        vin_data_cache = decode_vins_batch_cached(db, list(all_vins))
    else:
        vin_data_cache = {}

    for car_query in desired_cars:
        avg_table = db.get_averages(car_query)  # (year, title_grp) → (lo, hi)
        candidates = all_candidates[car_query]

        # Pre-compute trim averages per title group (avoids recomputing
        # inside the per-listing loop)
        trim_avgs_by_group = {}
        for grp in ("clean", "rebuilt", "salvage", "lemon"):
            trim_avgs_by_group[grp] = _build_trim_averages(
                candidates, car_query, mileage_threshold, target_group=grp)

        now = datetime.utcnow()

        for row in candidates:
            href = row["href"]
            price = row["price"]
            mileage = row["mileage"]
            year = row["year"]
            location = row["location"]

            if not year:
                continue
            if location_filter and location_filter not in (location or ""):
                continue

            mileage = mileage or 0

            # ── Title-group-aware average lookup ─────────────────
            # Compare this listing against others with the same title
            # status.  Falls back to "all" if not enough data.
            grp = title_group(row["title_type"])
            avg_key = (year, grp)
            if avg_key not in avg_table:
                avg_key = (year, "all")
            if avg_key not in avg_table:
                continue

            avg_lower, avg_higher = avg_table[avg_key]
            avg_price = avg_lower if mileage <= mileage_threshold else avg_higher

            if avg_price <= 0:
                continue

            # Trim tier for this listing
            trim_tier, trim_label = get_trim_tier(
                row["car_name"], car_query, row["trim"] or "")

            # Trim-specific average from same title group
            trim_avgs = trim_avgs_by_group.get(grp, {})
            trim_avg = trim_avgs.get((year, trim_tier))

            # Look up NHTSA rating
            nhtsa_key = (car_query.lower(), year)
            nhtsa_rating = nhtsa_cache.get(nhtsa_key)

            # Detect drivetrain
            dt, dt_source = detect_drivetrain(row["car_name"], car_query)

            # Days listed
            days_listed = 0
            if row["created_at"]:
                try:
                    created = datetime.fromisoformat(row["created_at"])
                    days_listed = max(0, (now - created).days)
                except (ValueError, TypeError):
                    pass

            # VIN cross-validation
            vin_mismatches = None
            vin = row["vin"]
            if vin:
                vd = vin_data_cache.get(vin.upper())
                if vd:
                    val_result = validate_vin_against_listing(vd, {
                        "year": year,
                        "car_query": car_query,
                        "drivetrain": drivetrain_label(dt),
                        "drivetrain_source": dt_source,
                    })
                    vin_mismatches = val_result["mismatches"]

            score_data = compute_deal_score(
                price=price,
                avg_price=avg_price,
                mileage=mileage,
                year=year,
                deal_rating=row["deal_rating"],
                accident_history=row["accident_history"],
                title_type=row["title_type"],
                nhtsa_rating=nhtsa_rating,
                trim_tier=trim_tier,
                trim_avg_price=trim_avg,
                drivetrain=dt,
                dt_source=dt_source,
                days_listed=days_listed,
                car_query=car_query,
                vin_mismatches=vin_mismatches,
            )
            score = score_data["total"]

            # Look up recalls and MPG for this car/year
            recall_key = (car_query.lower(), year)
            recalls = recalls_cache.get(recall_key, [])

            mpg_data = mpg_cache.get(recall_key)
            monthly_fuel_cost = None
            if mpg_data and mpg_data.get("mpg_combined"):
                monthly_fuel_cost = estimate_monthly_fuel_cost(
                    mpg_data["mpg_combined"])

            if score >= min_score:
                deals.append({
                    "href": href,
                    "price": price,
                    "mileage": mileage,
                    "year": year,
                    "location": location,
                    "source": row["source"],
                    "car_query": car_query,
                    "avg_price": avg_price,
                    "trim_avg_price": trim_avg,
                    "image_url": row["image_url"],
                    "car_name": row["car_name"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "trim": row["trim"],
                    "seller": row["seller"],
                    "condition": row["condition"],
                    "deal_rating": row["deal_rating"],
                    "accident_history": row["accident_history"],
                    "distance": row["distance"],
                    "title_type": row["title_type"],
                    "vin": row["vin"],
                    "trim_tier": trim_tier,
                    "trim_tier_name": tier_name(trim_tier),
                    "trim_label": trim_label,
                    "drivetrain": drivetrain_label(dt),
                    "drivetrain_source": dt_source,
                    "days_listed": days_listed,
                    "deal_score": score,
                    "deal_grade": score_to_grade(score),
                    "score_breakdown": score_data,
                    "nhtsa_rating": nhtsa_rating,
                    "title_cap": _title_cap(row["title_type"]),
                    "recalls": recalls,
                    "recalls_count": len(recalls),
                    "mpg_data": mpg_data,
                    "monthly_fuel_cost": monthly_fuel_cost,
                    "vin_mismatches": vin_mismatches,
                })

    # Deduplicate — same car posted under multiple URLs
    before = len(deals)
    deals = _dedup_deals(deals)
    dupes_removed = before - len(deals)
    if dupes_removed:
        logging.info(f"Removed {dupes_removed} duplicate listings.")

    logging.info(f"Found {len(deals)} deals (after dedup).")
    return deals
