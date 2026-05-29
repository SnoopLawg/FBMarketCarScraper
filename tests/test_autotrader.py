"""Regression tests for the Autotrader scraper.

Synthetic HTML matching autotrader card selectors. Note: mileage extraction
(`[class*='mileage']`) is known-broken in production (0% coverage) — these
tests lock in the working fields; a follow-up will fix mileage from
__NEXT_DATA__ once we can capture its inventory entry shape.
"""
import json

from bs4 import BeautifulSoup

from scrapers.autotrader import AutotraderScraper


MIN_CONFIG = {"MinPrice": 5000, "MaxPrice": 30000}


def _scrape_one(card, vin_map=None):
    captured = []
    scraper = AutotraderScraper(None, MIN_CONFIG,
                                lambda **kw: captured.append(kw),
                                car_list=["Honda CR-V"])
    ok = scraper._process_listing(card, "Honda CR-V", vin_map=vin_map)
    if not ok:
        return None
    assert len(captured) == 1
    return captured[0]


def _make_card(title="2020 Honda CR-V EX-L",
               href="/cars-for-sale/vehicle/769041357",
               price="$22,900",
               owner_text="1-Owner",
               accident_text="No Accidents Reported",
               distance_text="6.83 mi. away"):
    return BeautifulSoup(
        '<div data-cmp="inventoryListing">'
        f'<a href="{href}"><h2>{title}</h2></a>'
        f'<span data-cmp="firstPrice">{price}</span>'
        f'<span>{owner_text}</span>'
        f'<a>{accident_text}</a>'
        f'<span>{distance_text}</span>'
        '<img src="https://example.com/p.jpg"/>'
        '</div>',
        "html.parser",
    ).div


# ── Core extraction ───────────────────────────────────────────────


def test_card_extracts_title_price_href():
    row = _scrape_one(_make_card())
    assert row is not None
    assert row["car_name"] == "2020 Honda CR-V EX-L"
    assert row["price"] == "$22,900"
    assert row["href"] == "https://www.autotrader.com/cars-for-sale/vehicle/769041357"
    assert row["source"] == "autotrader"


def test_absolute_href_left_alone():
    full = "https://www.autotrader.com/cars-for-sale/vehicle/123"
    row = _scrape_one(_make_card(href=full))
    assert row["href"] == full


def test_distance_extracted_from_card_text():
    row = _scrape_one(_make_card(distance_text="12.5 mi. away"))
    assert row["distance"] == "12.5 mi"


def test_owner_count_extracted_from_card_text():
    row = _scrape_one(_make_card(owner_text="1-Owner Vehicle"))
    assert row["owner_count"] == "1"


def test_accident_history_picked_up():
    row = _scrape_one(_make_card(accident_text="No Accidents Reported"))
    assert "accident" in row["accident_history"].lower()


# ── Skip conditions ───────────────────────────────────────────────


def test_missing_title_returns_false():
    card = BeautifulSoup(
        '<div data-cmp="inventoryListing">'
        '  <a href="/x"><span>no h2 here</span></a>'
        '</div>',
        "html.parser").div
    assert _scrape_one(card) is None


# ── VIN map from __NEXT_DATA__ ────────────────────────────────────


def test_vin_resolved_when_listing_id_in_vin_map():
    vin_map = {"769041357": "5J6RT6H92NL057630"}
    row = _scrape_one(_make_card(href="/cars-for-sale/vehicle/769041357"),
                      vin_map=vin_map)
    assert row["vin"] == "5J6RT6H92NL057630"


def test_no_vin_when_listing_id_missing_from_map():
    row = _scrape_one(_make_card(), vin_map={"other_id": "VIN"})
    assert row["vin"] == ""


def test_extract_vin_map_parses_next_data_json():
    """Static method: synthetic __NEXT_DATA__ -> {listing_id: vin}."""
    payload = {
        "props": {"pageProps": {"__eggsState": {"inventory": {
            "111": {"vin": "VIN_AAA", "year": 2020},
            "222": {"vin": "VIN_BBB", "year": 2021},
            "333": {"year": 2019},  # no vin -> excluded
        }}}}
    }
    html = (f'<html><body>'
            f'<script id="__NEXT_DATA__" type="application/json">'
            f'{json.dumps(payload)}'
            f'</script></body></html>')
    soup = BeautifulSoup(html, "html.parser")
    vin_map = AutotraderScraper._extract_vin_map(soup)
    assert vin_map == {"111": "VIN_AAA", "222": "VIN_BBB"}


def test_extract_vin_map_handles_missing_script():
    soup = BeautifulSoup("<html><body>no script</body></html>", "html.parser")
    assert AutotraderScraper._extract_vin_map(soup) == {}


# ── Title-type detection from text ────────────────────────────────


def test_salvage_title_type_picked_up():
    card = _make_card(title="2018 Honda CR-V (SALVAGE TITLE)")
    row = _scrape_one(card)
    assert row["title_type"] == "salvage"
