"""Tests for sync_fb_cookies.py — the conversion + auth-cookie validation.

The actual browser_cookie3 read against your real Firefox is impossible to
test in CI (no Firefox profile present); these tests exercise the pure
logic: cookiejar→selenium-format conversion and required-cookie checking.
"""
from http.cookiejar import Cookie, CookieJar
from unittest.mock import patch

import pytest

import sync_fb_cookies as sfc


def _cookie(name, value, domain=".facebook.com", path="/", secure=True,
            expires=2_000_000_000):
    """Build a minimal http.cookiejar.Cookie for tests."""
    return Cookie(
        version=0, name=name, value=value, port=None, port_specified=False,
        domain=domain, domain_specified=True, domain_initial_dot=True,
        path=path, path_specified=True, secure=secure, expires=expires,
        discard=False, comment=None, comment_url=None,
        rest={}, rfc2109=False,
    )


def _jar(*cookies):
    jar = CookieJar()
    for c in cookies:
        jar.set_cookie(c)
    return jar


# ── cookies_to_selenium_format ────────────────────────────────────


def test_conversion_includes_all_selenium_required_fields():
    jar = _jar(_cookie("xs", "abc123"))
    out = sfc.cookies_to_selenium_format(jar)
    assert len(out) == 1
    c = out[0]
    for key in ("name", "value", "domain", "path", "secure", "expiry"):
        assert key in c
    assert c["name"] == "xs"
    assert c["value"] == "abc123"


def test_conversion_handles_no_expiry():
    """Session cookies (expires=None) should become expiry=None."""
    jar = _jar(_cookie("xs", "abc", expires=None))
    out = sfc.cookies_to_selenium_format(jar)
    assert out[0]["expiry"] is None


def test_conversion_coerces_secure_to_bool():
    jar = _jar(_cookie("xs", "abc", secure=1))
    out = sfc.cookies_to_selenium_format(jar)
    assert out[0]["secure"] is True


# ── main(): missing auth cookies short-circuits ──────────────────


def test_main_exits_nonzero_when_xs_missing(capsys):
    """If c_user is present but xs isn't, we're not really logged in."""
    fake = [{"name": "c_user", "value": "1", "domain": ".fb.com",
             "path": "/", "secure": True, "expiry": 1}]
    with patch.object(sfc, "read_firefox_cookies", return_value=fake):
        rc = sfc.main_for_test([])
    assert rc == 1


def test_main_exits_nonzero_when_c_user_missing():
    fake = [{"name": "xs", "value": "abc", "domain": ".fb.com",
             "path": "/", "secure": True, "expiry": 1}]
    with patch.object(sfc, "read_firefox_cookies", return_value=fake):
        rc = sfc.main_for_test([])
    assert rc == 1


def test_main_dry_run_succeeds_when_both_auth_cookies_present():
    fake = [
        {"name": "c_user", "value": "1", "domain": ".fb.com", "path": "/",
         "secure": True, "expiry": 1},
        {"name": "xs", "value": "abc", "domain": ".fb.com", "path": "/",
         "secure": True, "expiry": 1},
    ]
    with patch.object(sfc, "read_firefox_cookies", return_value=fake):
        rc = sfc.main_for_test(["--dry-run"])
    assert rc == 0
