"""FlareSolverr client — fetch Cloudflare-protected pages via a real browser.

Cars.com (and Autotrader) gate their detail pages behind Cloudflare, which
hard-blocks our headless Selenium/HTTP scrapers ("Attention Required"). A
self-hosted FlareSolverr container runs a real Chromium that solves the
challenge and returns the rendered HTML + a cf_clearance cookie.

Configured via the FLARESOLVERR_URL env var (e.g. http://flaresolverr:8191).
When unset, is_enabled() is False and callers skip FlareSolverr entirely, so
the app degrades gracefully on machines without it.

Usage:
    with FlareSolverrClient() as fs:      # one Cloudflare solve, reused
        if fs.enabled:
            html = fs.get(url)
"""

import logging
import os

import requests

DEFAULT_TIMEOUT_MS = 60000


def is_enabled():
    return bool(os.environ.get("FLARESOLVERR_URL"))


class FlareSolverrClient:
    def __init__(self, base_url=None, session_name="carscraper", recycle_every=15):
        self.base_url = (base_url or os.environ.get("FLARESOLVERR_URL") or "").rstrip("/")
        self.session_name = session_name
        # FlareSolverr's persistent browser leaks ~20-25 MB per request, so it
        # OOMs after ~40 fetches. Tear the session down and rebuild it every
        # `recycle_every` requests (and after any failure) to flush that memory.
        self.recycle_every = recycle_every
        self._req_count = 0
        self._session_id = None
        self._http = requests.Session()

    @property
    def enabled(self):
        return bool(self.base_url)

    def _post(self, payload, timeout):
        resp = self._http.post(f"{self.base_url}/v1", json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def open(self):
        """Create a reusable browser session so Cloudflare is solved once."""
        if not self.enabled or self._session_id:
            return
        try:
            data = self._post(
                {"cmd": "sessions.create", "session": self.session_name},
                timeout=120)
            if data.get("status") == "ok":
                self._session_id = self.session_name
        except Exception as e:
            logging.warning(f"[FlareSolverr] session create failed: {e}")

    def close(self):
        if not self._session_id:
            return
        try:
            self._post({"cmd": "sessions.destroy", "session": self._session_id},
                       timeout=30)
        except Exception as e:
            logging.warning(f"[FlareSolverr] session destroy failed: {e}")
        finally:
            self._session_id = None

    def _recycle(self):
        """Destroy and recreate the browser session to flush leaked memory."""
        self.close()
        self.open()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def get(self, url, max_timeout_ms=DEFAULT_TIMEOUT_MS):
        """Return solved HTML for url, or None on failure/block."""
        if not self.enabled:
            return None
        # Proactively recycle before the session grows large enough to OOM.
        if self._session_id and self._req_count and \
                self._req_count % self.recycle_every == 0:
            self._recycle()
        self._req_count += 1
        payload = {"cmd": "request.get", "url": url, "maxTimeout": max_timeout_ms}
        if self._session_id:
            payload["session"] = self._session_id
        try:
            data = self._post(payload, timeout=max_timeout_ms / 1000 + 30)
        except Exception as e:
            logging.warning(f"[FlareSolverr] get failed for {url[:60]}: {e}")
            # Session may be wedged/OOM — rebuild it for the next call.
            try:
                self._recycle()
            except Exception:
                pass
            return None
        if data.get("status") != "ok":
            logging.warning(f"[FlareSolverr] non-ok for {url[:60]}: {data.get('message')}")
            return None
        sol = data.get("solution") or {}
        if sol.get("status") and sol["status"] >= 400:
            return None
        return sol.get("response")
