"""KSL Cars scraper — HTTP-based, no Selenium needed.

KSL Cars (cars.ksl.com) is a Utah-local marketplace. Listings are
server-rendered via Next.js RSC with full JSON embedded in the HTML,
so we can extract structured data with a simple HTTP GET + parse.
"""

import json
import logging
import os
import re
import time
import random
from urllib.parse import urlparse

import requests

from scrapers.base import BaseScraper
from parsing import classify_seller_type, detect_title_type


class KSLScraper(BaseScraper):
    SOURCE_NAME = "ksl"
    NEEDS_DRIVER = False

    def __init__(self, driver, config, insert_fn, car_list=None):
        super().__init__(driver, config, insert_fn, car_list)
        self._session = requests.Session()
        self._apply_proxy(config)
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) "
                "Gecko/20100101 Firefox/128.0"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _apply_proxy(self, config):
        """Route KSL's HTTP through a proxy when configured.

        KSL is blocked by PerimeterX on datacenter IPs (the home/server IP),
        and FlareSolverr can't beat its press-and-hold captcha. A residential
        or mobile (4G/5G) IP is the durable fix. Resolution order:
          1. KSL_PROXY env var (easiest server override)
          2. Config Proxy.ksl  (a KSL-specific entry)
          3. Config Proxy.url / .urls (the general proxy)
        Accepts http(s)://, socks5:// (PySocks installed). No-op if unset, so
        this is safe until a proxy endpoint exists.
        """
        pc = (config or {}).get("Proxy") or {}
        url = (os.environ.get("KSL_PROXY") or pc.get("ksl") or pc.get("url")
               or (random.choice(pc["urls"]) if pc.get("urls") else ""))
        if url:
            self._session.proxies = {"http": url, "https": url}
            logging.info(f"[KSL] HTTP routed via proxy "
                         f"{urlparse(url).hostname or url}")

    def scrape(self):
        ksl_config = self.config["Sources"].get("ksl", {})
        max_pages = ksl_config.get("max_pages", 3)
        per_page = ksl_config.get("per_page", 48)

        total_found = 0
        total_matched = 0

        for i, car_query in enumerate(self.desired_cars):
            self.log(f"Scraping: {car_query}")
            if i > 0:
                time.sleep(random.uniform(3, 8))

            parts = car_query.strip().split()
            make = parts[0] if parts else ""
            model = " ".join(parts[1:]) if len(parts) > 1 else ""

            for page in range(max_pages):
                url = self._build_url(make, model, page, per_page)
                self.log(f"  Page {page + 1}...")

                if page > 0:
                    time.sleep(random.uniform(2, 5))

                try:
                    resp = self._session.get(url, timeout=30)
                    resp.raise_for_status()
                except Exception as e:
                    self.log(f"  Failed to fetch page {page + 1}: {e}")
                    break

                listings = self._extract_listings(resp.text)
                if not listings:
                    self.log(f"  No listings on page {page + 1}")
                    break

                total_found += len(listings)
                for listing in listings:
                    if self._process_listing(listing, car_query):
                        total_matched += 1

                self.log(f"  Page {page + 1}: {len(listings)} listings")

                if len(listings) < per_page:
                    break  # Last page

        self.log(f"Done: {total_found} total listings, {total_matched} inserted")

    def _build_url(self, make, model, page, per_page):
        """Build a KSL Cars search URL from parameters."""
        # KSL uses path-based params: /search/make/Toyota/model/Tacoma/...
        path = f"https://cars.ksl.com/search/make/{make}"
        if model:
            # URL-encode model for multi-word (e.g. "Cr-v" -> "Cr-v")
            path += f"/model/{requests.utils.quote(model)}"
        path += f"/priceFrom/{self.min_price}/priceTo/{self.max_price}"

        mileage_max = self.config.get("MileageMax")
        if mileage_max:
            path += f"/miles/{mileage_max}"

        path += f"/page/{page}/perPage/{per_page}"
        return path

    @staticmethod
    def _extract_listings(html):
        """Extract listing JSON objects from the Next.js RSC payload."""
        listings = []

        # KSL embeds data in RSC chunks: self.__next_f.push([1,"..."])
        # The chunk containing listings has "results":[[{...},...]]
        # Find the chunk with escaped "results" key
        pattern = re.compile(
            r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', re.DOTALL
        )
        for match in pattern.finditer(html):
            chunk = match.group(1)
            if '\\"results\\"' not in chunk:
                continue

            # Unescape the JSON string
            try:
                unescaped = chunk.encode('utf-8').decode('unicode_escape')
            except Exception:
                continue

            # Find the results array
            idx = unescaped.find('"results":[[')
            if idx < 0:
                continue

            # Extract from the opening of the results array
            start = idx + len('"results":')
            # Find the matching closing brackets ]]
            depth = 0
            end = start
            for j in range(start, len(unescaped)):
                if unescaped[j] == '[':
                    depth += 1
                elif unescaped[j] == ']':
                    depth -= 1
                    if depth == 0:
                        end = j + 1
                        break

            try:
                results_array = json.loads(unescaped[start:end])
                # Results is [[listing1, listing2, ...]] — unwrap outer array
                if results_array and isinstance(results_array[0], list):
                    listings = results_array[0]
                elif results_array and isinstance(results_array[0], dict):
                    listings = results_array
            except (json.JSONDecodeError, IndexError):
                pass

            if listings:
                break

        return listings

    def _process_listing(self, listing, car_query):
        """Process a single KSL listing dict and insert it."""
        try:
            listing_id = listing.get("id")
            if not listing_id:
                return False

            title = listing.get("title", "")
            price = listing.get("price")
            mileage = listing.get("mileage")
            vin = listing.get("vin", "")
            make_year = listing.get("makeYear")
            trim = listing.get("trim", "")
            seller_type_raw = listing.get("sellerType", "")

            # Location
            loc = listing.get("location", {})
            city = loc.get("city", "")
            state = loc.get("state", "")
            location = f"{city}, {state}" if city else ""

            # Image
            img = listing.get("primaryImage", {})
            image_url = img.get("url", "") if img else ""

            # Seller
            dealer = listing.get("dealer", {})
            seller = dealer.get("name", "") if dealer else ""

            # Seller type
            if seller_type_raw == "Dealership":
                seller_type = "dealer"
            elif seller_type_raw == "For Sale By Owner":
                seller_type = "fsbo"
            else:
                seller_type = classify_seller_type(
                    seller_name=seller, source="ksl") or ""

            # Href — KSL listing URL
            href = f"https://cars.ksl.com/listing/{listing_id}"

            # Price as string for parse_price
            price_str = str(int(price)) if price else ""

            # Mileage as string for parse_mileage
            mileage_str = f"{mileage} miles" if mileage else "N/A"

            self.counted_insert(
                car_query=car_query,
                href=href,
                image_url=image_url,
                price=price_str,
                car_name=title,
                location=location,
                mileage_raw=mileage_str,
                source=self.SOURCE_NAME,
                seller=seller,
                trim=trim,
                seller_type=seller_type,
                vin=vin,
            )
            return True
        except Exception as e:
            self.count_parse_error()
            logging.warning(f"[KSL] Parse error: {e}")
            return False

    # KSL detail pages embed the listing JSON with an escaped title field,
    # e.g.  \"titleType\":\"Rebuilt/Reconstructed Title\"  — the search-results
    # JSON we scrape omits it, so the detail page is the only place to read the
    # real title status. (Anchored on the leading \" so it can't match the
    # adjacent \"suggestedTitleType\" key.)
    _TITLE_TYPE_RE = re.compile(r'\\"titleType\\":\\"([^"\\]+)')

    class Blocked(Exception):
        """KSL's bot protection (PerimeterX) is rejecting us — back off."""

    def _fetch_title_type(self, href):
        """GET a KSL detail page and return (canonical_title_type, raw).

        Raises Blocked when the response is the bot wall rather than a
        listing page — the caller must NOT mark the row enriched (that's
        how 500 rows got burned as 'enriched, no title' when a high-volume
        run tripped the block) and should abort the batch.
        """
        resp = self._session.get(href, timeout=30)
        if resp.status_code in (403, 429):
            raise self.Blocked(f"HTTP {resp.status_code}")
        resp.raise_for_status()
        # A real listing page is a Next.js RSC document. The PerimeterX
        # interstitial ("Access to this page has been denied") has neither.
        if "__next_f" not in resp.text:
            raise self.Blocked("no RSC payload — bot wall or page rot")
        m = self._TITLE_TYPE_RE.search(resp.text)
        if not m:
            return None, None
        raw = m.group(1).strip()
        return detect_title_type(raw), raw

    def _enrich_batch(self, db, rows):
        """Fetch + store title_type for a batch of rows.

        Returns (updated, fetched, aborted). Only a successfully-read page
        with no title field is marked enriched; blocks and errors leave the
        row un-enriched for a later retry. Three consecutive blocks aborts
        the batch — hammering a wall just extends the block.
        """
        enriched = 0
        fetched = 0
        consecutive_blocks = 0
        for row in rows:
            fetched += 1
            href = row["href"]
            try:
                tt, raw = self._fetch_title_type(href)
                consecutive_blocks = 0
                if tt:
                    db.update_listing_details(href, title_type=tt)
                    enriched += 1
                    self.log(f"  Enriched: {row['car_name'][:40]} → title={tt} ({raw})")
                else:
                    # Page read fine, listing just doesn't state a title —
                    # mark done so we don't re-fetch it every run.
                    db.mark_enriched(href)
            except self.Blocked as e:
                consecutive_blocks += 1
                logging.warning(f"[KSL] blocked on {href[:60]}: {e} "
                                f"({consecutive_blocks}/3)")
                if consecutive_blocks >= 3:
                    logging.warning(
                        "[KSL] bot wall confirmed — aborting enrichment; "
                        "rows stay un-enriched and retry next run")
                    return enriched, fetched, True
                time.sleep(random.uniform(20, 40))
            except Exception as e:
                # Transient error — leave un-enriched for retry.
                logging.warning(f"[KSL] Title enrich error for {href[:60]}: {e}")
            time.sleep(random.uniform(2.0, 4.5))
        return enriched, fetched, False

    def enrich_listings(self, db, limit=40, max_total=80):
        """Capture title_type from KSL detail pages.

        The search-results JSON lacks the title status, so KSL listings land
        with title_type unset (which let a rebuilt car top the board
        uncapped). Detail pages are plain HTTP — but KSL fronts them with
        PerimeterX, and a high-volume run (450+ rapid fetches) got the
        server IP captcha-walled. So: modest per-run cap, polite delays, and
        a circuit breaker on block detection. At 4 scrapes/day this still
        converges on the backlog in ~2 days, and in steady state (only new
        listings each run) titles ARE first-run data.
        """
        total = 0
        fetched_total = 0
        while fetched_total < max_total:
            rows = db.get_listings_missing_title_type(
                source="ksl", limit=min(limit, max_total - fetched_total))
            if not rows:
                break
            if fetched_total == 0:
                self.log(f"Enriching KSL titles (up to {max_total} this run)...")
            updated, fetched, aborted = self._enrich_batch(db, rows)
            total += updated
            fetched_total += fetched
            if aborted:
                break
        if total:
            self.log(f"KSL title enrichment complete: {total} updated this run.")
        else:
            self.log("No KSL listings need title enrichment.")
        return total
        return enriched

    def log(self, msg):
        logging.info(f"[{self.SOURCE_NAME}] {msg}")
