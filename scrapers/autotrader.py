"""Autotrader scraper — per-car keyword search for targeted results.

Autotrader's Akamai Bot Manager hard-blocks vanilla Selenium/`requests`. The
durable, free bypass is **curl_cffi with browser TLS impersonation**: it carries
a real Chrome JA3 handshake, which passes Akamai (verified live — full search
page, 200, no block), so we fetch the search HTML over plain HTTP through the
shared adaptive fetcher (`netfetch`) and skip the browser AND FlareSolverr.

The search page embeds the full inventory as structured JSON in `__NEXT_DATA__`
(`props.pageProps.__eggsState.inventory`) — richer and far more stable than the
CSS cards (it carries drive type, exact mileage, VIN, distance, days-on-site).
We parse that JSON directly; the legacy CSS card parser (`_process_page`) is
kept only as a fallback when the embedded JSON is absent.
"""

import json
import re
import logging

from bs4 import BeautifulSoup

import netfetch
from scrapers.base import BaseScraper
from parsing import classify_seller_type


class AutotraderScraper(BaseScraper):
    SOURCE_NAME = "autotrader"
    # Pure HTTP via curl_cffi — no Selenium, no FlareSolverr.
    NEEDS_DRIVER = False

    def _build_search_url(self, car_query, zip_code, radius):
        """Autotrader keyword-search URL for a car query.

        e.g. /cars-for-sale/used-cars/toyota/tacoma?zip=...
        """
        parts = car_query.lower().split()
        make = parts[0] if parts else ""
        model = "-".join(parts[1:]) if len(parts) > 1 else ""
        return (
            f"https://www.autotrader.com/cars-for-sale/used-cars"
            f"/{make}/{model}"
            f"?zip={zip_code}&searchRadius={radius}"
            f"&minPrice={self.min_price}&maxPrice={self.max_price}"
        )

    @staticmethod
    def _is_blocked_html(html):
        """Akamai block page detection on raw HTML (FlareSolverr path)."""
        if not html:
            return True
        low = html[:20000].lower()
        return "akamai-block" in low or "page unavailable" in low

    def _process_page(self, html, car_query):
        """Parse one search-results page; returns (cards_found, inserted)."""
        soup = BeautifulSoup(html, "html.parser")
        vin_map = self._extract_vin_map(soup)
        cards = soup.select("[data-cmp='inventoryListing']")
        if not cards:
            cards = soup.select(".inventory-listing")
        matched = 0
        for card in cards:
            if self._process_listing(card, car_query, vin_map):
                matched += 1
        return len(cards), matched

    def scrape(self):
        """Fetch each car's search pages over curl_cffi (Akamai-passing TLS),
        parsing the embedded `__NEXT_DATA__` inventory JSON."""
        at_config = self.config["Sources"].get("autotrader", {})
        zip_code = at_config.get("zip", "84101")
        radius = at_config.get("search_radius", 100)
        max_pages = at_config.get("max_pages", 3)

        fetcher = netfetch.default_fetcher()
        total_found = 0
        total_matched = 0
        blocked_pages = 0

        for car_query in self.desired_cars:
            self.log(f"Scraping: {car_query}")
            base_url = self._build_search_url(car_query, zip_code, radius)

            for page in range(max_pages):
                url = (base_url if page == 0
                       else f"{base_url}&firstRecord={page * 25}")
                self.log(f"  Page {page + 1}...")

                res = fetcher.get(
                    url, domain="www.autotrader.com",
                    blocked_predicate=lambda r: self._is_blocked_html(r.text))
                if res.blocked or not res.ok:
                    blocked_pages += 1
                    self.count_parse_error()
                    logging.warning(
                        f"[autotrader] fetch failed/blocked (status "
                        f"{res.status}) for {car_query} page {page + 1} "
                        f"— skipping query")
                    break

                found, matched = self._process_search_html(res.text, car_query)
                if not found:
                    self.log(f"  No results on page {page + 1}")
                    break
                total_found += found
                total_matched += matched
                self.log(f"  Page {page + 1}: {found} listings, "
                         f"{matched} inserted")

        if blocked_pages:
            logging.warning(
                f"[autotrader] {blocked_pages} pages failed/blocked this run")
        self.log(f"Done: {total_found} total listings, "
                 f"{total_matched} inserted")

    def _process_search_html(self, html, car_query):
        """Parse one search page: prefer the embedded inventory JSON, fall
        back to the legacy CSS card parser. Returns (found, inserted)."""
        inv = self._extract_inventory(html)
        if inv:
            matched = 0
            for rec in inv.values():
                if self._insert_record(rec, car_query):
                    matched += 1
            return len(inv), matched
        # No JSON (page shape changed?) — fall back to CSS cards.
        return self._process_page(html, car_query)

    @staticmethod
    def _extract_inventory(html):
        """Return the `__eggsState.inventory` {id: record} dict, or {}."""
        soup = BeautifulSoup(html, "html.parser")
        script = soup.select_one("script#__NEXT_DATA__")
        if not script:
            return {}
        try:
            data = json.loads(script.string or "")
            inv = (data.get("props", {})
                       .get("pageProps", {})
                       .get("__eggsState", {})
                       .get("inventory", {}))
            return inv if isinstance(inv, dict) else {}
        except (json.JSONDecodeError, TypeError, AttributeError):
            return {}

    def _insert_record(self, rec, car_query):
        """Map one inventory JSON record to a listing insert. Returns True
        if inserted."""
        try:
            if not isinstance(rec, dict) or not rec.get("id"):
                return False
            lid = rec["id"]
            href = f"https://www.autotrader.com/cars-for-sale/vehicle/{lid}"
            car_name = rec.get("title") or rec.get("titleLong") or ""
            if not car_name:
                return False

            pricing = rec.get("pricingDetail") or {}
            price = pricing.get("displayPrice") or pricing.get("salePrice") or ""

            mileage_raw = (rec.get("mileage") or {}).get("value", "")
            trim = rec.get("atTrim") or (rec.get("trim") or {}).get("name", "") or ""
            image_url = self._first_image(rec)
            vin = self._extract_vin(rec)
            seller = rec.get("ownerName", "") or ""
            location = self._location_from_title(
                rec.get("titleLong", ""), car_name)
            distance = self._distance(rec)
            carfax_url = self._history_url(rec)
            seller_type = classify_seller_type(
                seller_name=seller, source="autotrader") or ""

            self.counted_insert(
                car_query=car_query, href=href, image_url=image_url,
                price=str(price), car_name=car_name, location=location,
                mileage_raw=str(mileage_raw), source=self.SOURCE_NAME,
                seller=seller, distance=distance, trim=trim,
                carfax_url=carfax_url, seller_type=seller_type, vin=vin,
            )
            return True
        except Exception as e:
            self.count_parse_error()
            logging.warning(f"[Autotrader] JSON record parse error: {e}")
            return False

    @staticmethod
    def _first_image(rec):
        sources = (rec.get("images") or {}).get("sources") or []
        return sources[0].get("src", "") if sources else ""

    @staticmethod
    def _extract_vin(rec):
        """VIN is embedded in the payment/incentive URLs of the record."""
        m = re.search(r"vin=([A-HJ-NPR-Z0-9]{17})", json.dumps(rec))
        return m.group(1) if m else ""

    @staticmethod
    def _distance(rec):
        d = (rec.get("marketExtension") or {}).get("distance")
        return f"{d:.1f} mi" if isinstance(d, (int, float)) else ""

    @staticmethod
    def _location_from_title(title_long, title=""):
        """`titleLong` is `title` + ' City ST ZIP'. Strip the known title
        prefix to isolate the location, then pull 'City, ST' (cities can be
        multi-word, e.g. 'Salt Lake City')."""
        if not title_long:
            return ""
        tail = title_long
        if title and title_long.startswith(title):
            tail = title_long[len(title):]
        m = re.search(r"^\s*(.+?)\s+([A-Z]{2})\s+\d{5}\s*$", tail)
        return f"{m.group(1).strip()}, {m.group(2)}" if m else ""

    @staticmethod
    def _history_url(rec):
        """AutoCheck/Carfax vehicle-history link from productTiles."""
        for tile in rec.get("productTiles") or []:
            link = (tile.get("link") or {}).get("href", "")
            if link and ("vehiclehistory" in link.lower()
                         or "carfax" in link.lower()):
                return link
        return ""

    @staticmethod
    def _extract_vin_map(soup):
        """Build {listing_id: vin} from __NEXT_DATA__ JSON on the page."""
        vin_map = {}
        script = soup.select_one("script#__NEXT_DATA__")
        if not script:
            return vin_map
        try:
            data = json.loads(script.string or "")
            inventory = (data.get("props", {})
                             .get("pageProps", {})
                             .get("__eggsState", {})
                             .get("inventory", {}))
            for lid, info in inventory.items():
                if isinstance(info, dict) and info.get("vin"):
                    vin_map[str(lid)] = info["vin"]
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
        return vin_map

    def _process_listing(self, card, car_query, vin_map=None):
        """Parse and insert a listing. Returns True if inserted."""
        try:
            # Title
            title_el = (
                card.select_one("h2")
                or card.select_one("[data-cmp='subheading']")
                or card.select_one("[class*='title']")
            )
            if not title_el:
                return False
            title = title_el.get_text(strip=True)

            # Link
            link_el = (
                card.select_one("a[href*='/cars-for-sale/vehicle']")
                or card.select_one("a[href*='/vehicledetails']")
                or card.select_one("a[href]")
            )
            href = ""
            if link_el:
                href = link_el.get("href", "")
                if href and not href.startswith("http"):
                    href = f"https://www.autotrader.com{href}"

            # Price
            price_el = (
                card.select_one("[data-cmp='firstPrice']")
                or card.select_one(".first-price")
                or card.select_one("[class*='price']")
            )
            price_str = price_el.get_text(strip=True) if price_el else ""

            # Mileage
            mileage_el = card.select_one("[class*='mileage']")
            mileage_str = mileage_el.get_text(strip=True) if mileage_el else "N/A"

            # Trim — often in a specs list as a short text like "LX", "SE", "EX"
            trim = ""
            for el in card.select(".text-overflow, [class*='trim']"):
                txt = el.get_text(strip=True)
                if txt and len(txt) < 30 and txt not in title:
                    trim = txt
                    break

            # Deal rating — e.g. "Great Price", "Good Price", "Fair Price"
            deal_rating = ""
            for el in card.select("[class*='deal'], [data-cmp*='deal'], [class*='badge']"):
                txt = el.get_text(strip=True)
                if txt and any(w in txt.lower() for w in ["price", "deal"]):
                    deal_rating = txt
                    break

            # Accident history — e.g. "No Accidents", "1 Accident"
            accident_history = ""
            for el in card.select("a, span, div"):
                txt = el.get_text(strip=True)
                if txt and "accident" in txt.lower():
                    accident_history = txt
                    break

            # Owner count — e.g. "1-Owner" badge
            owner_count = ""
            card_text = card.get_text(" ", strip=True)
            card_text_lower = card_text.lower()
            owner_match = re.search(r'(\d+)[- ]?owner', card_text_lower)
            if owner_match:
                owner_count = owner_match.group(1)

            # Carfax link — Autotrader often shows "Free CARFAX Report"
            carfax_url = ""
            for link in card.select("a[href*='carfax'], a[href*='CARFAX']"):
                carfax_url = link.get("href", "")
                if carfax_url:
                    break
            if not carfax_url:
                # Sometimes the link text mentions Carfax
                for link in card.select("a"):
                    if "carfax" in (link.get_text(strip=True) or "").lower():
                        carfax_url = link.get("href", "")
                        if carfax_url:
                            break

            # Title type — look for "clean title", "salvage", "rebuilt"
            title_type = ""
            if "salvage" in card_text_lower:
                title_type = "salvage"
            elif "rebuilt" in card_text_lower:
                title_type = "rebuilt"
            elif "lemon" in card_text_lower:
                title_type = "lemon"
            elif "clean title" in card_text_lower:
                title_type = "clean"

            # Seller name
            seller = ""
            for sel in ["[class*='dealer-name']", "[class*='seller']",
                        ".text-subdued", "[data-cmp*='dealer']"]:
                sel_el = card.select_one(sel)
                if sel_el:
                    txt = sel_el.get_text(strip=True)
                    if txt and len(txt) > 2:
                        seller = txt
                        break

            # Location / distance — e.g. "6.83 mi. away"
            location = ""
            distance = ""
            for el in card.select("[class*='dealer-name'], [class*='seller'], [class*='location']"):
                txt = el.get_text(strip=True)
                if txt:
                    location = txt
                    break
            # Look for distance pattern
            card_text = card.get_text()
            dist_match = re.search(r'([\d,.]+)\s*mi\.?\s*away', card_text)
            if dist_match:
                distance = f"{dist_match.group(1)} mi"

            # Image
            img_el = card.select_one("img")
            image_url = ""
            if img_el:
                image_url = img_el.get("src", "") or img_el.get("data-src", "")

            # VIN — look up from __NEXT_DATA__ inventory map
            vin = ""
            if vin_map and href:
                # Extract listing ID from href (e.g. /vehicle/769041357)
                lid_match = re.search(r'/vehicle/(\d+)', href)
                if lid_match:
                    vin = vin_map.get(lid_match.group(1), "")

            seller_type = classify_seller_type(
                seller_name=seller, source="autotrader") or ""

            self.counted_insert(
                car_query=car_query, href=href, image_url=image_url,
                price=price_str, car_name=title, location=location,
                mileage_raw=mileage_str, source=self.SOURCE_NAME,
                seller=seller, distance=distance, trim=trim,
                deal_rating=deal_rating, accident_history=accident_history,
                title_type=title_type, owner_count=owner_count,
                carfax_url=carfax_url, seller_type=seller_type, vin=vin,
            )
            return True
        except Exception as e:
            self.count_parse_error()
            logging.warning(f"[Autotrader] Parse error: {e}")
            return False
