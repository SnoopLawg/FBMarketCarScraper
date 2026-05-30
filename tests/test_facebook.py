"""Regression tests for the Facebook Marketplace scraper's _process_listing.

FB cards use specific div classes (`x1gslohp xkh6y0r`) for price/title/city/
mileage — synthetic HTML in this shape exercises the parsing without needing
to actually scrape FB (which bot-walls non-session traffic).
"""
from bs4 import BeautifulSoup

from scrapers.facebook import FacebookScraper


MIN_CONFIG = {"MinPrice": 5000, "MaxPrice": 30000}


def _make_card(price="$15,990", title="2021 Subaru Forester Premium",
               city="Salt Lake City, UT", miles="60K miles",
               include_miles=True, href="/marketplace/item/12345",
               img="https://example.com/photo.jpg"):
    divs = [
        f'<div class="x1gslohp xkh6y0r">{price}</div>',
        f'<div class="x1gslohp xkh6y0r">{title}</div>',
        f'<div class="x1gslohp xkh6y0r">{city}</div>',
    ]
    if include_miles:
        divs.append(f'<div class="x1gslohp xkh6y0r">{miles}</div>')
    a_open = f'<a href="{href}">' if href else ""
    a_close = "</a>" if href else ""
    img_tag = f'<img src="{img}"/>' if img else ""
    html = f'<div>{a_open}{img_tag}{"".join(divs)}{a_close}</div>'
    return BeautifulSoup(html, "html.parser").div


def _scrape_one(card):
    captured = []
    scraper = FacebookScraper(None, MIN_CONFIG,
                              lambda **kw: captured.append(kw),
                              car_list=["Subaru Forester"])
    scraper._process_listing(card, "Subaru Forester")
    return captured[0] if captured else None


# ── Core parsing ──────────────────────────────────────────────────


def test_four_divs_extracts_price_title_city_miles():
    row = _scrape_one(_make_card())
    assert row is not None
    assert row["price"] == "15,990"   # stripped "$" prefix
    assert row["car_name"] == "2021 Subaru Forester Premium"
    assert row["location"] == "Salt Lake City, UT"
    assert row["mileage_raw"] == "60K miles"
    assert row["source"] == "facebook"


def test_three_divs_yields_mileage_n_a():
    row = _scrape_one(_make_card(include_miles=False))
    assert row is not None
    assert row["mileage_raw"] == "N/A"


def test_href_made_absolute():
    row = _scrape_one(_make_card(href="/marketplace/item/abc"))
    assert row["href"] == "https://www.facebook.com/marketplace/item/abc"


def test_href_left_alone_if_already_absolute():
    row = _scrape_one(_make_card(href="https://www.facebook.com/marketplace/item/abc"))
    assert row["href"] == "https://www.facebook.com/marketplace/item/abc"


# ── Skip conditions (return without inserting) ────────────────────


def test_card_with_no_link_is_skipped():
    # No href -> no <a> tag
    html = ('<div>'
            '<img src="https://example.com/p.jpg"/>'
            '<div class="x1gslohp xkh6y0r">$10,000</div>'
            '<div class="x1gslohp xkh6y0r">2020 Forester</div>'
            '<div class="x1gslohp xkh6y0r">Provo, UT</div>'
            '<div class="x1gslohp xkh6y0r">80K miles</div>'
            '</div>')
    card = BeautifulSoup(html, "html.parser").div
    assert _scrape_one(card) is None


def test_card_with_no_image_is_skipped():
    html = ('<div><a href="/marketplace/item/x">'
            '<div class="x1gslohp xkh6y0r">$10,000</div>'
            '<div class="x1gslohp xkh6y0r">2020 Forester</div>'
            '<div class="x1gslohp xkh6y0r">Provo, UT</div>'
            '<div class="x1gslohp xkh6y0r">80K miles</div>'
            '</a></div>')
    card = BeautifulSoup(html, "html.parser").div
    assert _scrape_one(card) is None


def test_wrong_div_count_is_skipped():
    # Only 2 divs — not 3 or 4
    html = ('<div><a href="/x"><img src="x"/>'
            '<div class="x1gslohp xkh6y0r">$10,000</div>'
            '<div class="x1gslohp xkh6y0r">2020 Forester</div>'
            '</a></div>')
    card = BeautifulSoup(html, "html.parser").div
    assert _scrape_one(card) is None


def test_price_without_dollar_sign_is_skipped():
    row = _scrape_one(_make_card(price="Free"))
    assert row is None


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
