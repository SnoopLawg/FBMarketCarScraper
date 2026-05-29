"""Regression tests for the Craigslist scraper's _process_listing.

Synthetic HTML in Craigslist's various card shapes (multiple title selectors
are supported, meta text has the mileage). No live scrape needed.
"""
from bs4 import BeautifulSoup

from scrapers.craigslist import CraigslistScraper


MIN_CONFIG = {"MinPrice": 5000, "MaxPrice": 30000}


def _scrape_one(item, region="saltlakecity"):
    captured = []
    scraper = CraigslistScraper(None, MIN_CONFIG,
                                lambda **kw: captured.append(kw),
                                car_list=["Honda CR-V"])
    ok = scraper._process_listing(item, region, "Honda CR-V")
    if not ok:
        return None
    assert len(captured) == 1
    return captured[0]


def _make_item(title="2015 Honda CR-V EX",
               href="/cto/d/honda-crv/7822334455.html",
               price="$10,500",
               meta="2/19 95k mi (West Valley)",
               img="https://example.com/p.jpg",
               title_class="posting-title"):
    return BeautifulSoup(
        f'<li class="cl-search-result">'
        f'  <a class="{title_class}" href="{href}">{title}</a>'
        f'  <span class="result-price">{price}</span>'
        f'  <span class="meta">{meta}</span>'
        f'  <img src="{img}"/>'
        f'</li>',
        "html.parser",
    ).li


# ── Core extraction ───────────────────────────────────────────────


def test_full_item_extracts_core_fields():
    row = _scrape_one(_make_item())
    assert row is not None
    assert row["car_name"] == "2015 Honda CR-V EX"
    assert row["price"] == "$10,500"
    # meta "2/19 95k mi (West Valley)" -> mileage "95k mi"
    assert "95k" in row["mileage_raw"].lower()
    assert row["source"] == "craigslist"


def test_relative_href_made_absolute_with_region():
    row = _scrape_one(_make_item(href="/cto/d/x/123.html"), region="saltlakecity")
    assert row["href"] == "https://saltlakecity.craigslist.org/cto/d/x/123.html"


def test_absolute_href_left_alone():
    full = "https://saltlakecity.craigslist.org/cto/d/x/123.html"
    row = _scrape_one(_make_item(href=full))
    assert row["href"] == full


# ── Skip conditions ───────────────────────────────────────────────


def test_missing_title_returns_false():
    item = BeautifulSoup('<li class="cl-search-result"></li>',
                         "html.parser").li
    assert _scrape_one(item) is None


def test_empty_href_returns_false():
    item = BeautifulSoup(
        '<li><a class="posting-title" href="">title</a></li>',
        "html.parser").li
    assert _scrape_one(item) is None


# ── Meta parsing ──────────────────────────────────────────────────


def test_mileage_parses_with_k_suffix():
    row = _scrape_one(_make_item(meta="3/22 120k mi (Provo)"))
    assert "120k" in row["mileage_raw"].lower()


def test_meta_without_mileage_leaves_na():
    row = _scrape_one(_make_item(meta="3/22 (Ogden)"))
    assert row["mileage_raw"] == "N/A"


def test_location_extracted_from_meta_after_stripping_date_and_miles():
    row = _scrape_one(_make_item(meta="3/22 95k mi (West Jordan)"))
    # Either "West Jordan" or "(West Jordan)" depending on stripping
    assert "West Jordan" in row["location"]


# ── Title-type detection from listing text ────────────────────────


def test_salvage_keyword_picked_up():
    row = _scrape_one(_make_item(title="2018 Honda Civic — SALVAGE title, runs great"))
    assert row["title_type"] == "salvage"


def test_clean_title_keyword_picked_up():
    row = _scrape_one(_make_item(title="2019 Honda CR-V LX, clean title"))
    assert row["title_type"] == "clean"


# ── Multiple title selector fallbacks ─────────────────────────────


def test_title_via_result_title_class():
    item = BeautifulSoup(
        '<li>'
        '  <a class="result-title" href="/x/123.html">2020 CR-V Touring</a>'
        '  <span class="result-price">$22,000</span>'
        '  <span class="meta">3/22 45k mi (Lehi)</span>'
        '</li>',
        "html.parser").li
    row = _scrape_one(item)
    assert row is not None
    assert row["car_name"] == "2020 CR-V Touring"
