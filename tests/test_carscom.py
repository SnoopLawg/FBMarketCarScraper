"""Regression tests for the cars.com scraper's _process_listing extraction.

These lock in the fixes shipped after the spark->fuse design-system migration:
  - read price/trim/mileage/vin from the `data-vehicle-details` JSON blob
  - skip the `price-drop` badge and payment estimates
  - reject any parsed price below the configured MinPrice (floor backstop)

Fixtures are real cards captured from the live results page.
"""
import json
from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.carscom import CarsComScraper, _is_adjustment_amount


FIXTURES = Path(__file__).parent / "fixtures"
MIN_CONFIG = {"MinPrice": 5000, "MaxPrice": 30000}


def _load_card(name):
    html = (FIXTURES / name).read_text()
    return BeautifulSoup(html, "html.parser").select_one("[data-listing-id]")


def _scrape_one(card):
    """Run _process_listing with a capturing insert; return the captured row
    or None if the listing was filtered out."""
    captured = []
    scraper = CarsComScraper(None, MIN_CONFIG,
                             lambda **kw: captured.append(kw),
                             car_list=["Honda CR-V"])
    ok = scraper._process_listing(card, "Honda CR-V")
    if not ok:
        return None
    assert len(captured) == 1
    return captured[0]


# ── Fixture-based regression tests ────────────────────────────────


def test_plain_card_extracts_core_fields_from_json():
    """Normal card resolves price/trim/mileage/vin from the JSON blob."""
    card = _load_card("fixture_carscom_plain.html")
    row = _scrape_one(card)
    assert row is not None
    price = int(row["price"])
    assert price >= 5000, "price came back below the floor"
    assert row["trim"], "trim was 0% pre-fix — must be populated now"
    assert row["mileage_raw"]
    assert row["vin"] and len(row["vin"]) == 17

    raw = card.get("data-vehicle-details") or ""
    if raw:
        vd = json.loads(raw)
        assert price == int(float(vd["price"]))
        assert row["trim"] == vd["trim"]
        assert row["vin"] == vd["vin"]


def test_price_drop_card_uses_real_sale_price_not_drop_amount():
    """When a `price-drop` badge is on the card, the *real* sale price wins —
    NOT the drop amount (the original phantom-deal bug)."""
    card = _load_card("fixture_carscom_pricedrop.html")
    row = _scrape_one(card)
    assert row is not None, "price-drop card should not be filtered"
    price = int(row["price"])
    assert price >= 5000, f"drop amount leaked in as price: {price}"

    # Sanity: the card actually has the price-drop badge we're protecting
    # against. If a future fixture refresh removes it this test still passes,
    # but we surface the change loudly via this guard.
    has_drop = bool(card.select("[class*='price-drop']")) or \
        bool(card.find(string=lambda t: t and "price drop" in t.lower()))
    assert has_drop, ("price-drop fixture no longer contains a price-drop "
                      "badge — refresh tests/fixtures/")


def test_deal_rating_extracted_from_fuse_badge_when_present():
    card = _load_card("fixture_carscom_plain.html")
    row = _scrape_one(card)
    badge = card.select_one("fuse-badge")
    if badge and any(w in badge.get_text(strip=True).lower()
                     for w in ("deal", "fair price", "high price", "overpriced")):
        assert row["deal_rating"], "fuse-badge deal rating was not captured"


# ── Unit tests on the adjustment-amount helper ────────────────────


def test_is_adjustment_amount_flags_price_drop_class():
    el = BeautifulSoup(
        '<div class="price-drop"><span>$447</span></div>',
        "html.parser").div
    assert _is_adjustment_amount(el)


def test_is_adjustment_amount_flags_ancestor_with_drop_class():
    inner = BeautifulSoup(
        '<div class="datum-icon price-drop"><span>$100</span></div>',
        "html.parser").span
    assert _is_adjustment_amount(inner)


def test_is_adjustment_amount_flags_payment_text():
    el = BeautifulSoup(
        '<fuse-button>Est. $531/mo</fuse-button>',
        "html.parser").select_one("fuse-button")
    assert _is_adjustment_amount(el)


def test_is_adjustment_amount_leaves_real_price_alone():
    el = BeautifulSoup(
        '<span class="fuse-body-larger">$18,995</span>',
        "html.parser").span
    assert not _is_adjustment_amount(el)


# ── Min-price floor backstop ──────────────────────────────────────


def test_floor_rejects_implausibly_low_price():
    """If the only extractable price is below MinPrice, the listing is
    skipped — guards against any future selector that grabs an adjustment
    amount slipping past the class/text checks."""
    html = '''<fuse-card data-listing-id="x">
      <a href="/vehicledetail/x"><h2>Used 2020 Honda CR-V</h2></a>
      <span class="fuse-body-larger">$300</span>
    </fuse-card>'''
    card = BeautifulSoup(html, "html.parser").select_one("[data-listing-id]")
    assert _scrape_one(card) is None
