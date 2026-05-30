"""Shared requests.Session with retry/backoff for the public enrichment APIs
(NHTSA / EPA / VIN). One transient 5xx or socket hiccup used to lose the
entire scrape's enrichment for that car/year; now we retry with backoff.
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Retry on the common transient HTTP statuses + connection errors.
# backoff_factor=0.5 gives delays of 0.5, 1, 2, 4 seconds between retries
# (urllib3 caps at backoff_max=120s).
_RETRY = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET", "POST"}),
    raise_on_status=False,
)
_ADAPTER = HTTPAdapter(max_retries=_RETRY)

session = requests.Session()
session.mount("http://", _ADAPTER)
session.mount("https://", _ADAPTER)
# Override the default `python-requests/...` UA — some public APIs treat that
# as a low-priority bot signal. setdefault wouldn't replace it (Session sets
# its own default), so assign directly.
session.headers["User-Agent"] = (
    "CarScraper/1.0 (+https://github.com/SnoopLawg/FBMarketCarScraper)"
)


def get(url, **kwargs):
    """Convenience wrapper — `from http_client import get` for a one-import API."""
    return session.get(url, **kwargs)


def post(url, **kwargs):
    return session.post(url, **kwargs)
