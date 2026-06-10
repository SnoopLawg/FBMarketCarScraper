"""Regression tests for the Facebook Marketplace scraper's _process_listing.

FB's generated CSS classes rotate between builds, so the parser reads the
structural shape instead: each result card is an <a href="/marketplace/
item/<id>/…"> anchor whose span texts carry price, title, "City, ST" and
"NNNK miles" (each field rendered in several nested copies). The synthetic
cards here mirror real captures from a live search page (June 2026).
"""
from bs4 import BeautifulSoup

from scrapers.facebook import FacebookScraper


MIN_CONFIG = {"MinPrice": 5000, "MaxPrice": 30000}


def _make_card(price="$15,990", title="2021 Subaru Forester Premium",
               city="Salt Lake City, UT", miles="60K miles",
               include_miles=True,
               href="/marketplace/item/12345/?ref=search&referral_code=null",
               img="https://example.com/photo.jpg", lead_spans=()):
    # FB renders each field in several nested copies — simulate the dupes
    spans = list(lead_spans) + [price, title, title, city, city]
    if include_miles:
        spans.append(miles)
    img_tag = f'<img src="{img}"/>' if img else ""
    inner = "".join(f"<span>{s}</span>" for s in spans)
    html = f'<a href="{href}">{img_tag}{inner}</a>'
    return BeautifulSoup(html, "html.parser").a


def _scrape_one(card, seen_ids=None):
    captured = []
    scraper = FacebookScraper(None, MIN_CONFIG,
                              lambda **kw: captured.append(kw),
                              car_list=["Subaru Forester"])
    scraper._process_listing(card, "Subaru Forester", seen_ids)
    return captured[0] if captured else None


# ── Core parsing ──────────────────────────────────────────────────


def test_extracts_price_title_city_miles():
    row = _scrape_one(_make_card())
    assert row is not None
    assert row["price"] == "15,990"   # stripped "$" prefix
    assert row["car_name"] == "2021 Subaru Forester Premium"
    assert row["location"] == "Salt Lake City, UT"
    assert row["mileage_raw"] == "60K miles"
    assert row["source"] == "facebook"
    assert row["image_url"] == "https://example.com/photo.jpg"


def test_missing_mileage_yields_n_a():
    row = _scrape_one(_make_card(include_miles=False))
    assert row is not None
    assert row["mileage_raw"] == "N/A"


def test_href_canonicalized_to_item_url_without_tracking_params():
    row = _scrape_one(_make_card(
        href="/marketplace/item/1540041667515431/?ref=search&__tn__=x"))
    assert row["href"] == (
        "https://www.facebook.com/marketplace/item/1540041667515431/")


def test_discounted_listing_uses_current_price():
    """Price-drop cards prepend a combined "$new$old" span before the
    individual price spans — exactly as captured live."""
    row = _scrape_one(_make_card(
        price="$9,950", lead_spans=("$9,950$10,950",),
        title="2005 Toyota tacoma access cab TRD Off-Road"))
    assert row is not None
    assert row["price"] == "9,950"


def test_foreign_currency_prefix_parsed():
    row = _scrape_one(_make_card(price="MX$10,200",
                                 title="Toyota Tacoma 2006 v6",
                                 include_miles=False))
    assert row is not None
    assert row["price"] == "10,200"


def test_dealership_suffix_sets_seller_type_and_cleans_mileage():
    row = _scrape_one(_make_card(miles="145K miles · Dealership"))
    assert row is not None
    assert row["seller_type"] == "dealer"
    assert row["mileage_raw"] == "145K miles"


def test_private_listing_has_blank_seller_type():
    row = _scrape_one(_make_card())
    assert row["seller_type"] == ""


def test_missing_image_still_inserts_with_blank_url():
    # Lazy-loaded cards may not have a thumbnail yet — keep the listing
    row = _scrape_one(_make_card(img=""))
    assert row is not None
    assert row["image_url"] == ""


def test_seen_ids_dedups_repeated_cards():
    seen = set()
    assert _scrape_one(_make_card(), seen) is not None
    assert _scrape_one(_make_card(), seen) is None


# ── Skip conditions (return without inserting) ────────────────────


def test_anchor_without_item_id_is_skipped():
    card = BeautifulSoup(
        '<a href="/marketplace/category/cars"><span>$10,000</span>'
        '<span>2020 Forester</span></a>', "html.parser").a
    assert _scrape_one(card) is None


def test_card_without_price_is_skipped():
    row = _scrape_one(_make_card(price="Free"))
    assert row is None


def test_card_with_only_price_is_skipped():
    card = BeautifulSoup(
        '<a href="/marketplace/item/99/"><span>$10,000</span></a>',
        "html.parser").a
    assert _scrape_one(card) is None


def test_title_containing_miles_not_mistaken_for_mileage():
    """A title like "2010 Tacoma low miles!" must not be classified as the
    mileage span (mileage must start with digits + K/miles)."""
    row = _scrape_one(_make_card(title="Tacoma super low miles runs great"))
    assert row is not None
    assert row["car_name"] == "Tacoma super low miles runs great"
    assert row["mileage_raw"] == "60K miles"


# ── Title-type detection from listing text ────────────────────────


def test_title_type_salvage_picked_up_from_card_text():
    row = _scrape_one(_make_card(title="2018 Honda Civic SALVAGE title"))
    assert row["title_type"] == "salvage"


def test_title_type_rebuilt_picked_up():
    row = _scrape_one(_make_card(title="2017 Mazda CX-5 (rebuilt title)"))
    assert row["title_type"] == "rebuilt"


def test_title_type_clean_picked_up():
    row = _scrape_one(_make_card(miles="120K miles · clean title"))
    assert row["title_type"] == "clean"


def test_title_type_blank_when_no_keywords():
    row = _scrape_one(_make_card())
    assert row["title_type"] == ""


# ── Login-state detector (added after the logged-out-anon-view bug) ──


def _fake_driver(html, url="https://facebook.com/marketplace"):
    """Minimal stand-in for a Selenium driver: just exposes page_source +
    current_url, which is all _is_logged_in() reads."""
    class _D:
        page_source = html
        current_url = url
    return _D()


def _new_scraper(driver):
    return FacebookScraper(driver, MIN_CONFIG, lambda **k: None,
                           car_list=["Honda CR-V"])


def test_is_logged_in_false_on_anon_marketplace_with_login_link():
    """The exact failure pattern from prod: marketplace renders without a
    `loginbutton` ID but has an `href="/login"` CTA — must be detected."""
    html = ('<html><body>Marketplace results '
            '<a href="/login/">Log In</a></body></html>')
    s = _new_scraper(_fake_driver(html))
    assert s._is_logged_in() is False


def test_is_logged_in_false_on_create_new_account_cta():
    html = ('<html><body>Marketplace results '
            '<span>Create New Account</span></body></html>')
    s = _new_scraper(_fake_driver(html))
    assert s._is_logged_in() is False


def test_is_logged_in_false_on_login_button():
    html = '<html><body><button id="loginbutton">Log In</button></body></html>'
    s = _new_scraper(_fake_driver(html))
    assert s._is_logged_in() is False


def test_is_logged_in_false_on_login_url():
    s = _new_scraper(_fake_driver("<html></html>",
                                  url="https://facebook.com/login/"))
    assert s._is_logged_in() is False


def test_is_logged_in_true_on_authenticated_marketplace():
    """Real logged-in page has no /login link and no signup CTA."""
    html = ('<html><body>Marketplace '
            '<a href="/messages">Messages</a></body></html>')
    s = _new_scraper(_fake_driver(html))
    assert s._is_logged_in() is True


# ── 2FA TOTP handling ─────────────────────────────────────────────


class _FakeField:
    def __init__(self):
        self.typed = ""
        self.sent_keys = []
    def clear(self):
        self.typed = ""
    def send_keys(self, val):
        self.typed += str(val)
        self.sent_keys.append(val)


class _FakeDriver2FA:
    """Stand-in driver: exposes the page state the helper inspects + lets
    tests observe what was typed into the (mocked) code field."""
    def __init__(self, on_2fa_page=True):
        self.current_url = (
            "https://www.facebook.com/two_step_verification/authentication/"
            if on_2fa_page else "https://www.facebook.com/"
        )
        self.page_source = ""
        self.code_field = _FakeField()
    def find_element(self, by, sel):
        # Pretend the input is always findable on the 2FA page
        return self.code_field
    def execute_script(self, *_a, **_kw): pass


def _scraper_with(driver):
    s = FacebookScraper(driver, MIN_CONFIG, lambda **k: None, car_list=["_"])
    # Suppress sleeps in the helper for fast tests
    s.human_delay = lambda *a, **k: None
    return s


def test_2fa_helper_returns_true_when_not_on_2fa_page():
    s = _scraper_with(_FakeDriver2FA(on_2fa_page=False))
    assert s._handle_2fa_if_present() is True


def test_2fa_helper_returns_false_when_secret_missing(monkeypatch):
    monkeypatch.delenv("FB_TOTP_SECRET", raising=False)
    s = _scraper_with(_FakeDriver2FA(on_2fa_page=True))
    assert s._handle_2fa_if_present() is False


def test_2fa_helper_submits_6digit_code_when_secret_set(monkeypatch):
    # Use a known base32 secret; the code is whatever pyotp computes now.
    monkeypatch.setenv("FB_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    drv = _FakeDriver2FA(on_2fa_page=True)
    s = _scraper_with(drv)
    # Sleep + delay inside the typing loop — neutralize them
    import scrapers.facebook as fb
    monkeypatch.setattr(fb.time, "sleep", lambda *_: None)
    assert s._handle_2fa_if_present() is True
    typed = drv.code_field.typed.rstrip("")  # strip RETURN keysym
    assert len(typed) == 6 and typed.isdigit(), (
        f"expected a 6-digit code typed, got {typed!r}")


def test_2fa_helper_strips_spaces_from_secret(monkeypatch):
    """Auth-app secrets are often shown with spaces (e.g. 'CMKQ GRNR …'); the
    helper must accept that copy-paste form."""
    monkeypatch.setenv("FB_TOTP_SECRET", "JBSW Y3DP EHPK 3PXP")
    drv = _FakeDriver2FA(on_2fa_page=True)
    s = _scraper_with(drv)
    import scrapers.facebook as fb
    monkeypatch.setattr(fb.time, "sleep", lambda *_: None)
    assert s._handle_2fa_if_present() is True
