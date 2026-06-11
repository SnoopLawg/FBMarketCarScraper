"""Tests for analysis.py — specifically the VIN-aware deal dedup that
prevents the same cross-posted car from being shown 2-3x on the board.
"""
from analysis import _dedup_deals


def _deal(href, source="ksl", vin="", car_name="2020 Honda CR-V", year=2020,
          price=20000, trim="", image_url="x", deal_rating=""):
    return {
        "href": href, "source": source, "vin": vin,
        "car_name": car_name, "year": year, "price": price,
        "trim": trim, "image_url": image_url, "deal_rating": deal_rating,
    }


def _hrefs(deals):
    return {d["href"] for d in deals}


# ── VIN-keyed dedup (the cross-source case) ──────────────────────


def test_same_vin_across_sources_collapses_to_one():
    """The exact case the VIN dedup commit fixed: a car cross-posted to
    cars.com and KSL would have shown twice on the board."""
    vin = "5J6RT6H92NL057630"
    deals = [
        _deal("https://ksl.com/listing/1", source="ksl", vin=vin),
        _deal("https://cars.com/vehicle/abc", source="carscom", vin=vin),
    ]
    out = _dedup_deals(deals)
    assert len(out) == 1, "same VIN should collapse to a single deal"


def test_richer_listing_kept_when_vin_matches():
    """When multiple deals share a VIN, keep the one with the most fields
    populated (richness heuristic)."""
    vin = "1HGCM82633A123456"
    sparse = _deal("https://a.com/1", source="carscom", vin=vin)
    rich = _deal("https://b.com/2", source="ksl", vin=vin,
                 trim="Touring", deal_rating="Great Deal")
    out = _dedup_deals([sparse, rich])
    assert len(out) == 1
    assert out[0]["href"] == "https://b.com/2"
    assert out[0]["trim"] == "Touring"


def test_different_vins_not_deduped():
    deals = [
        _deal("https://a/1", source="ksl", vin="VIN_AAA"),
        _deal("https://b/2", source="ksl", vin="VIN_BBB"),
    ]
    assert len(_dedup_deals(deals)) == 2


def test_vin_normalization_strips_whitespace_and_uppercases():
    """VINs from different sources may differ in case/whitespace — they
    must still be treated as the same key."""
    deals = [
        _deal("https://a/1", source="ksl", vin="  5J6rt6h92NL057630  "),
        _deal("https://b/2", source="carscom", vin="5J6RT6H92NL057630"),
    ]
    assert len(_dedup_deals(deals)) == 1


# ── Fallback dedup (no VIN, e.g. FB / Craigslist) ────────────────


def test_vinless_dedup_falls_back_to_name_year_price():
    """When VIN is missing, the (car_name, year, price) key still catches
    exact-text duplicates posted under different URLs."""
    a = _deal("https://fb.com/marketplace/item/1", source="facebook",
              vin="", car_name="2018 Ford Escape Titanium", year=2018, price=9400)
    b = _deal("https://fb.com/marketplace/item/2", source="facebook",
              vin="", car_name="2018 Ford Escape Titanium", year=2018, price=9400)
    out = _dedup_deals([a, b])
    assert len(out) == 1


def test_vinless_different_prices_not_deduped():
    a = _deal("https://fb/1", vin="", car_name="2018 Ford Escape", year=2018, price=9400)
    b = _deal("https://fb/2", vin="", car_name="2018 Ford Escape", year=2018, price=9500)
    assert len(_dedup_deals([a, b])) == 2


# ── Mixed-mode dedup ─────────────────────────────────────────────


def test_vin_takes_priority_over_name_match():
    """A VIN match dedups even if car_name strings disagree (which is the
    common case across sources that name cars differently)."""
    vin = "JF2SKAGC5RH412374"
    deals = [
        _deal("https://carscom/x", source="carscom", vin=vin,
              car_name="Used 2024 Subaru Forester Sport"),
        _deal("https://ksl/y", source="ksl", vin=vin,
              car_name="2024 Subaru Forester Sport AWD"),
    ]
    assert len(_dedup_deals(deals)) == 1


def test_empty_input_returns_empty():
    assert _dedup_deals([]) == []


# ── Sold-price weighting in averages ─────────────────────────────

from analysis import calculate_averages, SOLD_WEIGHT


class _FakeDB:
    """Captures upsert_average calls so we can assert on the computed mean."""
    def __init__(self, rows):
        self._rows = rows           # list of (price, mileage, year, tt, vin, sold)
        self.averages = {}          # (year, group) -> (avg_lower, avg_higher)

    def get_priced_listings(self, car_query):
        return self._rows

    def upsert_average(self, car_query, year, avg_lower, avg_higher, title_group):
        self.averages[(year, title_group)] = (avg_lower, avg_higher)

    def record_price_snapshot(self, *a, **k):
        pass


def test_sold_listing_dominates_the_average():
    """One sold comp at 10k plus three asking listings at 20k must pull the
    average far below the unweighted mean (~17.5k) — toward the sold price."""
    rows = [
        (20000, 30000, 2020, "clean", None, 0),
        (20000, 30000, 2020, "clean", None, 0),
        (20000, 30000, 2020, "clean", None, 0),
        (10000, 30000, 2020, "clean", None, 1),   # sold — weighted SOLD_WEIGHT
    ]
    db = _FakeDB(rows)
    calculate_averages(db, ["Toyota RAV4"], mileage_threshold=100000)
    avg_lower, _ = db.averages[(2020, "clean")]
    # Weighted: (3*20000 + SOLD_WEIGHT*10000) / (3 + SOLD_WEIGHT)
    expected = round((3 * 20000 + SOLD_WEIGHT * 10000) / (3 + SOLD_WEIGHT))
    assert avg_lower == expected
    assert avg_lower < 17500, "sold comp must pull the mean well below asking"


def test_all_asking_listings_is_plain_mean():
    rows = [
        (20000, 30000, 2021, "clean", None, 0),
        (22000, 30000, 2021, "clean", None, 0),
        (24000, 30000, 2021, "clean", None, 0),
    ]
    db = _FakeDB(rows)
    calculate_averages(db, ["Honda CR-V"], mileage_threshold=100000)
    avg_lower, _ = db.averages[(2021, "clean")]
    assert avg_lower == 22000


# ── Drivetrain scoring confidence (VIN-confirmed = fully confirmed) ──

from analysis import compute_deal_score


def _score(drivetrain, dt_source):
    """Score an otherwise-neutral deal and return its drivetrain factor."""
    return compute_deal_score(
        price=20000, avg_price=20000, mileage=40000, year=2022,
        deal_rating="", accident_history=None, title_type="clean",
        nhtsa_rating=None, drivetrain=drivetrain, dt_source=dt_source,
    )["drivetrain_score"]


def test_vin_confirmed_awd_scores_like_explicit():
    # A VIN-decoded drivetrain is ground truth — it earns the full
    # confirmed-AWD bonus, not the lower inferred score.
    assert _score("4wd", "vin") == 10.0
    assert _score("4wd", "explicit") == 10.0
    assert _score("4wd", "default") == 6.0


def test_vin_confirmed_fwd_scores_like_explicit():
    assert _score("fwd", "vin") == 2.0
    assert _score("fwd", "explicit") == 2.0
    assert _score("fwd", "default") == 3.0


def test_vin_confirmed_awd_reason_labels_source():
    reasons = compute_deal_score(
        price=20000, avg_price=20000, mileage=40000, year=2022,
        deal_rating="", accident_history=None, title_type="clean",
        nhtsa_rating=None, drivetrain="4wd", dt_source="vin",
    )["reasons"]
    assert "VIN-confirmed" in reasons["drivetrain"]


# ── Buyer guidance (the "how to handle this deal" panel) ──

from analysis import compute_buyer_guidance


def _guidance_deal(**over):
    base = {
        "price": 20000, "avg_price": 22000, "days_listed": 5,
        "seller_type": "private", "title_type": "clean",
        "price_history": None, "market_range": None, "recalls_count": 0,
        "owner_count": 1, "service_history": "records", "vin": "X" * 17,
        "vin_mismatches": None, "accident_history": "No Accidents",
    }
    base.update(over)
    return base


def test_guidance_offer_below_asking_and_ordered():
    g = compute_buyer_guidance(_guidance_deal())
    assert g["offer_open"] <= g["offer_target"] < 20000


def test_guidance_stale_listing_increases_room():
    fresh = compute_buyer_guidance(_guidance_deal(days_listed=2))
    stale = compute_buyer_guidance(_guidance_deal(days_listed=75))
    assert stale["room_pct"] > fresh["room_pct"]
    assert any("75 days" in l for l in stale["leverage"])


def test_guidance_bottom_of_market_clamps_room():
    g = compute_buyer_guidance(_guidance_deal(
        days_listed=80,
        market_range={"count": 12, "low": 21000, "fair": 23000, "high": 26000}))
    # Priced below the market low → don't haggle a steal away.
    assert g["room_pct"] <= 2
    assert any("bottom" in l.lower() for l in g["leverage"])


def test_guidance_unknown_title_asks_for_title():
    g = compute_buyer_guidance(_guidance_deal(title_type=None))
    assert any("title" in q.lower() for q in g["questions"])


def test_guidance_too_good_flags_scam_check():
    g = compute_buyer_guidance(_guidance_deal(price=14000, avg_price=22000))
    assert any("verify why" in f for f in g["red_flags"])


def test_guidance_sold_or_priceless_returns_none():
    assert compute_buyer_guidance(_guidance_deal(price=0)) is None
    assert compute_buyer_guidance(_guidance_deal(sold=True)) is None
