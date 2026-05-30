"""Sanity tests for the shared HTTP session — retries should be configured
so a transient 5xx on NHTSA/EPA/VIN no longer drops enrichment silently."""
from http_client import session


def test_session_has_https_adapter_with_retries():
    adapter = session.get_adapter("https://api.nhtsa.gov/")
    retry = adapter.max_retries
    assert retry.total == 3, "should retry transient failures up to 3x"
    assert 500 in retry.status_forcelist
    assert 502 in retry.status_forcelist
    assert 503 in retry.status_forcelist
    assert 504 in retry.status_forcelist
    assert 429 in retry.status_forcelist  # rate limit


def test_session_retry_methods_include_get_and_post():
    adapter = session.get_adapter("https://api.nhtsa.gov/")
    assert "GET" in adapter.max_retries.allowed_methods
    assert "POST" in adapter.max_retries.allowed_methods


def test_session_has_user_agent():
    assert "User-Agent" in session.headers
    assert "CarScraper" in session.headers["User-Agent"]
