"""Regression tests for the KSL scraper's _process_listing.

KSL takes a parsed JSON dict (not an HTML card), so tests are entirely
synthetic — no live fixture needed. Locks in the field mapping and the
seller-type classification.
"""
from scrapers.ksl import KSLScraper


MIN_CONFIG = {"MinPrice": 5000, "MaxPrice": 30000}


def _scrape_one(listing):
    """Run _process_listing with a capturing insert; return the captured row
    or None if filtered out."""
    captured = []
    scraper = KSLScraper(None, MIN_CONFIG,
                        lambda **kw: captured.append(kw),
                        car_list=["Honda CR-V"])
    ok = scraper._process_listing(listing, "Honda CR-V")
    if not ok:
        return None
    assert len(captured) == 1
    return captured[0]


def _full_listing(**overrides):
    base = {
        "id": 10606405,
        "title": "2015 Subaru Forester 2.5i Limited",
        "price": 5900,
        "mileage": 69500,
        "vin": "JF2SJAHC9FH515973",
        "makeYear": 2015,
        "trim": "2.5i Limited",
        "sellerType": "For Sale By Owner",
        "location": {"city": "Hurricane", "state": "UT"},
        "primaryImage": {"url": "https://example.com/photo.jpg"},
        "dealer": None,
    }
    base.update(overrides)
    return base


def test_full_listing_extracts_core_fields():
    row = _scrape_one(_full_listing())
    assert row is not None
    assert row["price"] == "5900"
    assert row["mileage_raw"] == "69500 miles"
    assert row["vin"] == "JF2SJAHC9FH515973"
    assert row["trim"] == "2.5i Limited"
    assert row["location"] == "Hurricane, UT"
    assert row["image_url"] == "https://example.com/photo.jpg"
    assert row["href"] == "https://cars.ksl.com/listing/10606405"
    assert row["source"] == "ksl"
    assert row["seller_type"] == "fsbo"


def test_dealership_seller_type():
    row = _scrape_one(_full_listing(
        sellerType="Dealership",
        dealer={"name": "A.I. Monroe Auto Sales"},
    ))
    assert row["seller_type"] == "dealer"
    assert row["seller"] == "A.I. Monroe Auto Sales"


def test_fsbo_seller_type():
    row = _scrape_one(_full_listing(sellerType="For Sale By Owner"))
    assert row["seller_type"] == "fsbo"


def test_unknown_seller_type_classifies_from_dealer_name():
    """Unknown sellerType + dealer dict falls through to classify_seller_type
    (which infers 'dealer' from common dealership name patterns)."""
    row = _scrape_one(_full_listing(
        sellerType="",
        dealer={"name": "Larry H. Miller Honda"},
    ))
    assert row["seller_type"] in ("dealer", "fsbo", "")  # tolerant of classifier


def test_missing_id_returns_false():
    listing = _full_listing()
    listing.pop("id")
    assert _scrape_one(listing) is None


def test_missing_price_yields_empty_price_string():
    row = _scrape_one(_full_listing(price=None))
    assert row is not None
    assert row["price"] == ""


def test_href_is_built_from_listing_id():
    row = _scrape_one(_full_listing(id=999888777))
    assert row["href"] == "https://cars.ksl.com/listing/999888777"


def test_location_omitted_when_city_missing():
    row = _scrape_one(_full_listing(location={"city": "", "state": "UT"}))
    assert row["location"] == ""


# ── Title-type extraction from detail page (the rebuilt-car miss) ──

from parsing import detect_title_type


def _title_from_detail(escaped_json):
    """Apply the detail-page title regex the way _fetch_title_type does."""
    m = KSLScraper._TITLE_TYPE_RE.search(escaped_json)
    return detect_title_type(m.group(1)) if m else None


def test_detail_rebuilt_title_extracted_and_mapped():
    # Real KSL detail JSON shape: titleType sits next to suggestedTitleType.
    blob = (r'\"mileage\":25742,\"titleType\":\"Rebuilt/Reconstructed Title\",'
            r'\"suggestedTitleType\":{\"suggestedTitleType\":\"Clean Title\"}')
    assert _title_from_detail(blob) == "rebuilt"


def test_detail_regex_does_not_match_suggested_title_type():
    # Must read titleType, not the adjacent suggestedTitleType (which is "Clean").
    blob = (r'\"titleType\":\"Salvage Title\",'
            r'\"suggestedTitleType\":{\"suggestedTitleType\":\"Clean Title\"}')
    assert _title_from_detail(blob) == "salvage"


def test_detail_clean_and_unspecified_titles():
    assert _title_from_detail(r'\"titleType\":\"Clean Title\"') == "clean"
    # "Not Specified" has no canonical mapping → None (stays unknown).
    assert _title_from_detail(r'\"titleType\":\"Not Specified\"') is None


def test_dismantled_title_maps_to_salvage():
    assert detect_title_type("Dismantled Title") == "salvage"
