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
