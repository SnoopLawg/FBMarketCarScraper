"""Tests for parsing.py utility functions.

These are pure functions used by every scraper — a regression silently
breaks every listing's data.
"""
import pytest

from parsing import (parse_price, parse_mileage, extract_year,
                     parse_owner_count, classify_seller_type,
                     detect_title_type)
from analysis import title_group


# ── detect_title_type (accuracy: catch real titles, reject boilerplate) ──

def test_title_clean_from_fb_structured_field():
    # FB "About this vehicle" → "Clean title / no significant damage"
    assert detect_title_type(
        "clean title this vehicle has no significant damage or problems") == "clean"


def test_title_rebuilt_from_branded_description():
    # Real FB seller description: "REBUILT / BRANDED TITLE"
    assert detect_title_type(
        "great truck. rebuilt / branded title. 2.7l 4 cyl") == "rebuilt"


def test_title_branded_alone_is_rebuilt():
    assert detect_title_type("this truck has a branded title") == "rebuilt"


def test_title_salvage_phrase():
    assert detect_title_type("sold with a salvage title, runs great") == "salvage"


def test_title_lemon_only_on_buyback_phrase():
    assert detect_title_type("manufacturer buyback lemon law buyback") == "lemon"


def test_title_lemon_law_disclaimer_is_NOT_lemon():
    """The Cars.com / FB 'Lemon Law' boilerplate must not flag a car as a
    lemon — this produced 55+ bogus F-caps."""
    assert detect_title_type(
        "by using this site you agree to your state's lemon law rights") is None


def test_title_bare_salvage_word_is_not_salvage():
    assert detect_title_type("plenty of salvage yards near you") is None


# ── "title is {status}" phrasing (AutoSavvy etc.) ──

def test_title_is_branded_reversed_phrasing_is_rebuilt():
    # The exact AutoSavvy template the fixed phrase list missed.
    assert detect_title_type(
        "The current status of the title is branded, highlighting "
        "AutoSavvy's commitment to transparency") == "rebuilt"


def test_title_status_is_salvage():
    assert detect_title_type("Title status is salvage") == "salvage"


def test_title_is_clean_reversed_phrasing():
    assert detect_title_type("the title is clean and well maintained") == "clean"


def test_branded_word_without_title_context_is_not_flagged():
    # 'branded' in product prose must NOT trip rebuilt without a title nearby.
    assert detect_title_type("premium branded audio system, leather seats") is None


def test_branded_audio_but_clean_title_resolves_clean():
    assert detect_title_type("branded floor mats and the title is clean") == "clean"


def test_title_absent_returns_none():
    assert detect_title_type("driven 80,000 miles, automatic, gasoline") is None


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


# ── Powertrain detection (hybrid/EV comps must not mix with gas) ──

from parsing import detect_powertrain


def test_powertrain_hybrid_from_name():
    assert detect_powertrain("2021 Toyota RAV4 Hybrid XLE") == "hybrid"
    assert detect_powertrain("2020 Honda CR-V", trim="Hybrid Touring") == "hybrid"


def test_powertrain_phev_variants():
    assert detect_powertrain("2022 Toyota RAV4 Prime XSE") == "phev"
    assert detect_powertrain("2021 Ford Escape Plug-In Hybrid") == "phev"
    assert detect_powertrain("2023 Jeep Wrangler 4xe") == "phev"


def test_powertrain_ev_from_vin_fuel_and_models():
    assert detect_powertrain("2022 Ford Mustang Mach-E") == "ev"
    assert detect_powertrain("2021 Chevy Bolt EV") == "ev"
    assert detect_powertrain("2023 Hyundai Kona", vin_fuel="Electric") == "ev"


def test_powertrain_gas_default_and_no_false_positives():
    assert detect_powertrain("2019 Toyota RAV4 XLE AWD") == ""
    assert detect_powertrain("2020 Honda CR-V EX-L") == ""
