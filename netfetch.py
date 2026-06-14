"""Shared adaptive HTTP fetch layer — the rate-limit core for all sources.

Today every scraper re-implements its own `consecutive_blocked >= 5 -> break`
counter, paces with blind fixed sleeps, ignores `Retry-After`, and *abandons*
its queue the moment it gets throttled. This module centralizes that into one
well-behaved fetcher every source can route through:

  - Per-domain **token bucket** — steady-state pacing, one bucket per host
    (different sites tolerate different rates). This is also what turns a
    4-burst-a-day cron into a smooth trickle.
  - **Exponential backoff with full jitter** on a block/429 — desynchronizes
    retries instead of hammering.
  - Honors **Retry-After** when the server sends it (the exact wait, not a guess).
  - **Back-off-and-resume, not break** — a transient throttle retries the same
    request; only a *sustained* wall trips a per-domain **circuit breaker** that
    cools the host for a while (the event is meant to surface in /api/health).
  - **AIMD** — additively raise a domain's rate on a clean streak, multiplicatively
    cut it on a block. The pace self-tunes per run.

Transport is curl_cffi (`impersonate=`), so requests carry a real browser's
TLS/JA3 fingerprint — which is what lets plain HTTP reach endpoints that block
vanilla `requests` (Akamai/Cloudflare/PerimeterX fingerprint the handshake
before any JS runs). FlareSolverr/Selenium become last-resort, not the default.

Thread-safe: the worker may eventually run sources concurrently, and the
per-domain buckets are process-global state.
"""

import logging
import random
import threading
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

from curl_cffi import requests as creq

DEFAULT_IMPERSONATE = "chrome"


@dataclass
class _DomainState:
    """Per-domain token-bucket + AIMD + circuit-breaker state."""
    tokens: float = 0.0
    rate: float = 0.0          # current refill rate (tokens/sec); set on init
    last_refill: float = 0.0   # monotonic timestamp of last refill
    consecutive_blocks: int = 0
    breaker_until: float = 0.0  # monotonic deadline; 0 = closed


class RateLimiter:
    """Per-domain adaptive rate limiter (token bucket + AIMD + breaker)."""

    def __init__(self, *, rate=0.5, burst=3.0, min_rate=0.1, max_rate=2.0,
                 ai_step=0.1, md_factor=0.5, breaker_threshold=5,
                 breaker_cooldown=120.0, max_sleep_chunk=5.0):
        # rate: requests/sec steady-state per domain (0.5 == one every 2s)
        self.init_rate = rate
        self.burst = burst
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.ai_step = ai_step
        self.md_factor = md_factor
        self.breaker_threshold = breaker_threshold
        self.breaker_cooldown = breaker_cooldown
        self.max_sleep_chunk = max_sleep_chunk
        self._states = {}
        self._lock = threading.Lock()

    def _state(self, domain):
        st = self._states.get(domain)
        if st is None:
            st = _DomainState(tokens=self.burst, rate=self.init_rate,
                              last_refill=time.monotonic())
            self._states[domain] = st
        return st

    def acquire(self, domain):
        """Block until a token is free for `domain` (and the breaker is closed)."""
        while True:
            with self._lock:
                st = self._state(domain)
                now = time.monotonic()
                if st.breaker_until and now < st.breaker_until:
                    wait = st.breaker_until - now
                else:
                    st.breaker_until = 0.0
                    st.tokens = min(self.burst,
                                    st.tokens + (now - st.last_refill) * st.rate)
                    st.last_refill = now
                    if st.tokens >= 1.0:
                        st.tokens -= 1.0
                        return
                    wait = (1.0 - st.tokens) / st.rate
            # Sleep OUTSIDE the lock so other domains aren't blocked; chunk long
            # breaker waits so state stays re-checkable.
            time.sleep(min(wait, self.max_sleep_chunk))

    def on_success(self, domain):
        """Clean response — AIMD additive increase, reset the block streak."""
        with self._lock:
            st = self._state(domain)
            st.rate = min(self.max_rate, st.rate + self.ai_step)
            st.consecutive_blocks = 0

    def on_block(self, domain, retry_after=None):
        """Blocked/throttled — AIMD multiplicative decrease; trip the breaker
        after a sustained streak (honoring Retry-After for the cooldown)."""
        with self._lock:
            st = self._state(domain)
            st.rate = max(self.min_rate, st.rate * self.md_factor)
            st.consecutive_blocks += 1
            if st.consecutive_blocks >= self.breaker_threshold:
                cooldown = max(self.breaker_cooldown, retry_after or 0.0)
                st.breaker_until = time.monotonic() + cooldown
                logging.warning(
                    "[netfetch] circuit breaker OPEN for %s — cooling %.0fs "
                    "after %d consecutive blocks",
                    domain, cooldown, st.consecutive_blocks)

    def breaker_open(self, domain):
        with self._lock:
            st = self._states.get(domain)
            return bool(st and st.breaker_until
                        and time.monotonic() < st.breaker_until)


@dataclass
class FetchResult:
    status: int
    url: str
    text: str = ""
    content: bytes = b""
    blocked: bool = False
    from_cache: bool = False
    error: str = None
    headers: dict = field(default_factory=dict)

    @property
    def ok(self):
        return (not self.blocked) and 200 <= self.status < 400


# HTTP statuses that almost always mean "slow down / you're blocked".
_BLOCK_STATUSES = {403, 429, 503}


class Fetcher:
    """Adaptive, fingerprint-impersonating HTTP client built on a RateLimiter.

    Reuses one curl_cffi Session per Fetcher so connections AND cookies persist
    (Akamai's bm_sz/_abck sensor cookies accumulate across requests, which
    raises the trust score on later calls).
    """

    def __init__(self, *, limiter=None, impersonate=DEFAULT_IMPERSONATE,
                 headers=None, max_retries=4, base_backoff=2.0,
                 backoff_cap=60.0, timeout=40):
        self.limiter = limiter or RateLimiter()
        self.impersonate = impersonate
        self.headers = headers or {}
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.backoff_cap = backoff_cap
        self.timeout = timeout
        self.session = creq.Session()

    @staticmethod
    def _parse_retry_after(resp):
        val = resp.headers.get("Retry-After")
        if not val:
            return None
        try:
            return float(int(val))           # delta-seconds form
        except (TypeError, ValueError):
            pass
        try:                                  # HTTP-date form
            dt = parsedate_to_datetime(val)
            return max(0.0, dt.timestamp() - time.time())
        except (TypeError, ValueError):
            return None

    def _sleep_backoff(self, attempt, retry_after=None):
        if retry_after is not None:
            # Honor the server's instruction; small jitter to avoid lockstep.
            time.sleep(retry_after + random.uniform(0, 1.0))
            return
        # Full-jitter exponential backoff: uniform(0, base * 2**attempt), capped.
        ceiling = min(self.backoff_cap, self.base_backoff * (2 ** attempt))
        time.sleep(random.uniform(0, ceiling))

    def get(self, url, *, domain=None, headers=None, blocked_predicate=None):
        """GET `url` with adaptive pacing + backoff-and-resume.

        `blocked_predicate(resp) -> bool` lets a caller flag soft blocks that
        return HTTP 200 (e.g. an Akamai/CF challenge page served as 200).
        Returns a FetchResult; `.blocked` is True if every attempt was walled.
        """
        domain = domain or urlparse(url).netloc
        merged_headers = {**self.headers, **(headers or {})}
        last = None
        for attempt in range(self.max_retries + 1):
            self.limiter.acquire(domain)
            try:
                r = self.session.get(url, impersonate=self.impersonate,
                                     headers=merged_headers, timeout=self.timeout)
            except Exception as e:                      # network/timeout
                self.limiter.on_block(domain)
                last = FetchResult(status=0, url=url, blocked=True, error=str(e))
                if attempt < self.max_retries:
                    self._sleep_backoff(attempt)
                continue

            soft_blocked = bool(blocked_predicate and blocked_predicate(r))
            if r.status_code in _BLOCK_STATUSES or soft_blocked:
                retry_after = self._parse_retry_after(r)
                self.limiter.on_block(domain, retry_after=retry_after)
                last = FetchResult(status=r.status_code, url=url, text=r.text,
                                   blocked=True, headers=dict(r.headers))
                if attempt < self.max_retries:
                    self._sleep_backoff(attempt, retry_after=retry_after)
                continue

            self.limiter.on_success(domain)
            return FetchResult(status=r.status_code, url=url, text=r.text,
                               content=r.content, headers=dict(r.headers))
        return last or FetchResult(status=0, url=url, blocked=True)


# ── Process-wide shared fetcher ────────────────────────────────────────────
# Per-domain buckets must be shared across sources, so they all draw on one
# limiter. Scrapers call `netfetch.default_fetcher()`.
_shared = None
_shared_lock = threading.Lock()


def default_fetcher():
    global _shared
    if _shared is None:
        with _shared_lock:
            if _shared is None:
                _shared = Fetcher()
    return _shared
