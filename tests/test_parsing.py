"""Tests for parsing.py utility functions.

These are pure functions used by every scraper — a regression silently
breaks every listing's data.
"""
import pytest

from parsing import (parse_price, parse_mileage, extract_year,
                     parse_owner_count, classify_seller_type)
from analysis import title_group


# ── parse_price ───────────────────────────────────────────────────


@pytest.mark.parametrize("inp,expected", [
    ("$3,500", 3500.0),
    ("$22,995", 22995.0),
    ("12000", 12000.0),
    ("$1,234,567", 1234567.0),
    ("  $9,999  ", 9999.0),
])
def test_parse_price_valid(inp, expected):
    assert parse_price(inp) == expected


@pytest.mark.parametrize("inp", [None, "", "Sold", "Free", "Call for price"])
def test_parse_price_invalid_returns_none(inp):
    assert parse_price(inp) is None


# ── parse_mileage ─────────────────────────────────────────────────


def test_parse_mileage_with_k_suffix_multiplies():
    assert parse_mileage("120K miles") == 120000


def test_parse_mileage_with_lowercase_k():
    assert parse_mileage("95k mi") == 95000


def test_parse_mileage_with_comma():
    assert parse_mileage("142,500 miles") == 142500


def test_parse_mileage_bare_number():
    assert parse_mileage("46000") == 46000


def test_parse_mileage_under_1000_multiplied():
    # "Under 1000" is treated as thousands (e.g. "120" in "120 mi" is unusual,
    # but parsing assumes K when value is small to be safe).
    assert parse_mileage("120 mi") == 120000


def test_parse_mileage_invalid_returns_none():
    assert parse_mileage(None) is None
    assert parse_mileage("N/A") is None
    assert parse_mileage("") is None


# ── extract_year ──────────────────────────────────────────────────


@pytest.mark.parametrize("title,year", [
    ("2021 Subaru Forester Premium", 2021),
    ("Used 2015 Honda CR-V LX", 2015),
    ("1999 Toyota Tacoma", 1999),
    ("2024 Hyundai Santa Fe SEL", 2024),
])
def test_extract_year_finds_4digit_year(title, year):
    assert extract_year(title) == year


def test_extract_year_returns_none_when_no_year():
    assert extract_year("Honda CR-V") is None
    assert extract_year("") is None


def test_extract_year_ignores_non_year_4digits():
    # "1234" isn't a 19xx/20xx year — should be skipped
    assert extract_year("1234 random text") is None


# ── parse_owner_count ─────────────────────────────────────────────


@pytest.mark.parametrize("text,expected", [
    ("1-Owner Vehicle", 1),
    ("one owner", 1),
    ("1st Owner", 1),
    ("3 owners", 3),
])
def test_parse_owner_count_picks_up_common_phrasings(text, expected):
    assert parse_owner_count(text) == expected


def test_parse_owner_count_returns_none_when_absent():
    assert parse_owner_count("just some text about cars") is None


# ── classify_seller_type ──────────────────────────────────────────


def test_classify_seller_type_dealer_keywords_match():
    """Names containing dealer keywords (auto sales / dealership / automotive)
    classify as 'dealer' on cars.com / autotrader."""
    assert classify_seller_type(
        seller_name="ABC Auto Sales", source="carscom") == "dealer"
    assert classify_seller_type(
        seller_name="Murdock Automotive", source="autotrader") == "dealer"


def test_classify_seller_type_non_dealer_name_is_private():
    """Names without dealer keywords classify as 'private' on cars.com."""
    assert classify_seller_type(
        seller_name="John Smith", source="carscom") == "private"


def test_classify_seller_type_craigslist_cto_means_private():
    """Craigslist URLs with /cto/ (cars-trucks-by-owner) classify as 'private'."""
    result = classify_seller_type(
        href="https://saltlakecity.craigslist.org/cto/d/x/123.html",
        source="craigslist")
    assert result == "private"


def test_classify_seller_type_craigslist_ctd_means_dealer():
    """Craigslist URLs with /ctd/ (cars-trucks-by-dealer) classify as 'dealer'."""
    result = classify_seller_type(
        href="https://saltlakecity.craigslist.org/ctd/d/x/123.html",
        source="craigslist")
    assert result == "dealer"


def test_classify_seller_type_unknown_source_returns_none():
    assert classify_seller_type(seller_name="anything", source="unknown") is None


# ── analysis.title_group ──────────────────────────────────────────


def test_title_group_known_brand_titles():
    assert title_group("salvage") == "salvage"
    assert title_group("rebuilt") == "rebuilt"
    assert title_group("lemon") == "lemon"


def test_title_group_clean_and_unknown_collapse_together():
    """Clean and null/unknown both go to the 'clean' bucket so unknown-title
    listings are scored against the broadest comparable cohort."""
    assert title_group("clean") == "clean"
    assert title_group(None) == "clean"
    assert title_group("") == "clean"
    assert title_group("anything else") == "clean"


def test_title_group_case_insensitive():
    assert title_group("SALVAGE") == "salvage"
    assert title_group("Rebuilt") == "rebuilt"
