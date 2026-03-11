"""Listing cleanup, average price calculation, and deal scoring."""

import logging
from collections import defaultdict
from datetime import datetime

from nhtsa import parse_make_model, get_vehicle_rating
from trim_tiers import get_trim_tier, tier_name
from drivetrain import detect_drivetrain, drivetrain_label, is_awd_or_4wd


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

def compute_deal_score(price, avg_price, mileage, year, deal_rating,
                       accident_history, title_type, nhtsa_rating,
                       trim_tier=1, trim_avg_price=None,
                       drivetrain="unknown", dt_source="unknown",
                       days_listed=0):
    """Compute a composite deal score from 0-100 with 7 factors.

    Title & Condition is the DOMINANT factor (25 pts) because a bad
    title (salvage/rebuilt) tanks a car's real value 30-60%.  Hard score
    caps ensure salvage cars can *never* grade well regardless of price.

    Factors:
      - Price vs Average: 30pts
      - Title & Condition: 25pts  ← THE biggest factor
      - Mileage: 15pts (absolute benchmarks)
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

    # Use trim-specific average if available, fall back to overall
    effective_avg = trim_avg_price if trim_avg_price and trim_avg_price > 0 else avg_price

    # ── Price factor (30 points max) ──────────────────────────────
    if effective_avg and effective_avg > 0 and price < effective_avg:
        price_ratio = (effective_avg - price) / effective_avg
        price_score = round(min(30.0, price_ratio * 80.0), 1)

    # ── Title & Condition factor (25 points max) ──────────────────
    # This is the BIGGEST factor.  A salvage title is a dealbreaker;
    # a clean title is the baseline expectation for a real deal.
    #
    # Title base:  clean=18, unknown=4, rebuilt/salvage/lemon=0
    # Accidents:   no accident=+5, reported=0
    # Deal rating: great=+2, good=+1
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

    # Accident history
    accident_pts = 0.0
    if accident_history and "no accident" in (accident_history or "").lower():
        accident_pts = 5.0

    # Deal rating from marketplace
    rating_pts = 0.0
    if deal_rating:
        rating = deal_rating.lower()
        if "great" in rating:
            rating_pts = 2.0
        elif "good" in rating:
            rating_pts = 1.0

    condition_score = round(min(25.0, title_pts + accident_pts + rating_pts), 1)

    # ── Mileage factor (15 points max) — absolute benchmarks ──────
    if mileage is not None and mileage > 0:
        if mileage < 30000:
            mileage_score = 15.0
        elif mileage < 60000:
            mileage_score = round(15.0 - (mileage - 30000) / 30000 * 1.5, 1)
        elif mileage < 100000:
            mileage_score = round(13.5 - (mileage - 60000) / 40000 * 3.0, 1)
        elif mileage < 130000:
            mileage_score = round(10.5 - (mileage - 100000) / 30000 * 4.0, 1)
        elif mileage < 160000:
            mileage_score = round(6.5 - (mileage - 130000) / 30000 * 4.0, 1)
        else:
            mileage_score = 0.0
        mileage_score = max(0.0, mileage_score)

    # ── Reliability factor (10 points max) — NHTSA data ───────────
    if nhtsa_rating:
        stars = nhtsa_rating.get("overall_rating")
        if stars is not None:
            star_points = {5: 5.0, 4: 4.0, 3: 3.0, 2: 1.5, 1: 0.5}
            reliability_score += star_points.get(stars, 2.5)
        else:
            reliability_score += 2.5

        complaints = nhtsa_rating.get("complaints_count", 0)
        if complaints < 50:
            reliability_score += 5.0
        elif complaints < 100:
            reliability_score += 4.0
        elif complaints < 200:
            reliability_score += 2.5
        elif complaints < 400:
            reliability_score += 1.0
    else:
        reliability_score = 5.0

    reliability_score = round(min(10.0, reliability_score), 1)

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

    # ── Trim Value factor (5 points max) ──────────────────────────
    if trim_tier >= 2 and avg_price and avg_price > 0:
        overall_discount = (avg_price - price) / avg_price if price < avg_price else 0
        tier_mult = {2: 0.5, 3: 0.75, 4: 1.0}.get(trim_tier, 0)
        trim_score = round(min(5.0, overall_discount * 20.0 * tier_mult), 1)
    trim_score = max(0.0, trim_score)

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

    # ── Raw total ─────────────────────────────────────────────────
    raw_total = round(price_score + condition_score + mileage_score
                      + reliability_score + drivetrain_score + trim_score
                      + freshness_score, 1)

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
            )
            score = score_data["total"]

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
                })

    # Deduplicate — same car posted under multiple URLs
    before = len(deals)
    deals = _dedup_deals(deals)
    dupes_removed = before - len(deals)
    if dupes_removed:
        logging.info(f"Removed {dupes_removed} duplicate listings.")

    logging.info(f"Found {len(deals)} deals (after dedup).")
    return deals
