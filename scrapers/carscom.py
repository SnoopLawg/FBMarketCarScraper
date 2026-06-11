"""Cars.com scraper — per-car keyword search for targeted results."""

import json
import re
import logging
import time
import random
from urllib.parse import quote_plus

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from parsing import classify_seller_type, parse_price, detect_title_type
from vin import extract_vin
from driver import create_driver


def _is_adjustment_amount(el):
    """True if a price element shows a price-drop amount or financing estimate
    rather than the sale price.

    Cars.com (fuse design system) puts the sale price in `fuse-body-larger`, a
    "price drop" badge in a `price-drop` wrapper (the dollar amount is a bare
    inner <span> with no class), and a payment estimate ("Est. $531/mo") in a
    <fuse-button>. Detect drops via the element's own class or any ancestor
    whose class contains "drop"; detect payments via text markers.
    """
    classes = " ".join(el.get("class") or []).lower()
    if "drop" in classes or el.find_parent(class_=re.compile("drop", re.I)):
        return True
    txt = el.get_text(" ", strip=True).lower()
    return any(m in txt for m in ("/mo", "mo.", "/month", "month", "payment",
                                  "est.", "reduc"))


class CarsComScraper(BaseScraper):
    SOURCE_NAME = "carscom"

    def scrape(self):
        cc_config = self.config["Sources"].get("carscom", {})
        zip_code = cc_config.get("zip", "84101")
        max_dist = cc_config.get("max_distance", 100)
        max_pages = cc_config.get("max_pages", 3)

        total_found = 0
        total_matched = 0

        # Randomize query order each run (defensive — spreads any per-run
        # failures across queries rather than always starving the same ones).
        cars = list(self.desired_cars)
        random.shuffle(cars)
        original_driver = self.driver

        for i, car_query in enumerate(cars):
            self.log(f"Scraping: {car_query}")
            if i > 0:
                self.delay_between_searches()

            base_url = (
                f"https://www.cars.com/shopping/results/"
                f"?zip={zip_code}&maximum_distance={max_dist}"
                f"&list_price_max={self.max_price}&list_price_min={self.min_price}"
                f"&stock_type=used&keyword={quote_plus(car_query)}"
                f"&sort=best_match_desc"
            )

            # Fresh driver per query. Cars.com's TLS handshake degrades after a
            # handful of navigations in one Firefox session, so every query
            # after the first would fail (nssFailure2) and return no cards —
            # which held price coverage at ~44%. A new session per query keeps
            # coverage complete; pagination within a query stays in one session.
            self.driver = create_driver(proxy_config=self.config.get("Proxy"))
            try:
                for page in range(max_pages):
                    url = base_url if page == 0 else f"{base_url}&page={page + 1}"
                    self.log(f"  Page {page + 1}...")
                    if page > 0:
                        self.human_delay(8, 15)

                    cards = self._fetch_cards(url, retries=1 if page == 0 else 0)
                    if not cards:
                        if page == 0:
                            self.log(f"  No results for '{car_query}'")
                        break

                    total_found += len(cards)
                    for card in cards:
                        if self._process_listing(card, car_query):
                            total_matched += 1

                    self.log(f"  Page {page + 1}: {len(cards)} listings")
            finally:
                try:
                    self.driver.quit()
                except Exception:
                    pass
                self.driver = original_driver

        self.log(f"Done: {total_found} total listings, {total_matched} inserted")

    def _fetch_cards(self, url, retries=0):
        """Load a results URL and return its listing-card elements.

        Cars.com throttles consecutive searches in a session — a throttled
        request returns a page with no cards. Retry with a longer backoff to
        recover before giving up on the query.
        """
        for attempt in range(retries + 1):
            try:
                self.driver.get(url)
            except Exception as e:
                self.log(f"  Failed to load: {e}")
                return []
            self.human_delay(4, 8)
            self.scroll_page(count=4)
            try:
                WebDriverWait(self.driver, 12).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR,
                         "spark-card[data-listing-id], "
                         "[data-listing-id], .vehicle-card")))
            except Exception:
                pass
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            cards = soup.select("spark-card[data-listing-id]")
            if not cards:
                cards = [el for el in soup.select("[data-listing-id]")
                         if el.select_one("a[href*='/vehicledetail']")]
            if not cards:
                cards = soup.select(".vehicle-card")
            if cards:
                return cards
            if attempt < retries:
                self.log(f"  Empty page, retry {attempt + 1}/{retries} "
                         f"after backoff...")
                self.human_delay(12, 25)
        return []

    def _process_listing(self, card, car_query):
        """Parse and insert a listing. Returns True if inserted."""
        try:
            # Title & link
            link_el = (
                card.select_one("a[href*='/vehicledetail']")
                or card.select_one("a.vehicle-card-link")
                or card.select_one("a[href]")
            )
            if not link_el:
                return False
            title = link_el.get_text(strip=True)
            if not title:
                title_el = card.select_one("h2") or card.select_one("[class*='title']")
                title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                return False

            href = link_el.get("href", "")
            if href and not href.startswith("http"):
                href = f"https://www.cars.com{href}"

            # Structured data — cars.com embeds a per-listing JSON blob in the
            # `data-vehicle-details` attribute (price, trim, drivetrain, mileage,
            # vin, ...). It survives design-system changes, so it's the primary
            # source; the CSS selectors below are fallbacks for when it's absent.
            vd = {}
            raw_vd = card.get("data-vehicle-details", "")
            if not raw_vd:
                parent = card.find_parent(attrs={"data-vehicle-details": True})
                raw_vd = parent.get("data-vehicle-details", "") if parent else ""
            if raw_vd:
                try:
                    vd = json.loads(raw_vd)
                except (json.JSONDecodeError, TypeError):
                    vd = {}

            # Price — prefer the JSON price; else the fuse sale-price span
            # (`.primary-price`/`spark-body-larger` are legacy). The broad
            # `[class*='price']` selector only matches the "price drop" badge
            # now, so it's last and adjustment amounts are skipped.
            price_str = str(vd["price"]) if vd.get("price") else ""
            if not price_str:
                for sel in ["span.fuse-body-larger", ".primary-price",
                            "span.spark-body-larger", "[class*='price']"]:
                    for price_el in card.select(sel):
                        txt = price_el.get_text(strip=True)
                        if "$" not in txt or not any(c.isdigit() for c in txt):
                            continue
                        if _is_adjustment_amount(price_el):
                            continue
                        price_str = txt
                        break
                    if price_str:
                        break

            # Mileage — prefer the JSON value (an int), else the visible text.
            mileage_str = "N/A"
            if vd.get("mileage"):
                mileage_str = str(vd["mileage"])
            else:
                mileage_node = card.find(string=lambda t: t and "mi." in t.lower())
                if mileage_node:
                    mileage_str = mileage_node.strip()

            # Seller
            seller = ""
            for sel in ["[class*='dealer']", "[class*='seller']"]:
                sel_el = card.select_one(sel)
                if sel_el:
                    seller = sel_el.get_text(strip=True)
                    break

            # Location + distance (e.g., "Mount Aire, UT (11 mi)")
            location = ""
            distance = ""
            loc_el = card.select_one(".datum-icon:not(.mileage)")
            if loc_el:
                loc_text = loc_el.get_text(strip=True)
                dist_match = re.search(r'\((\d+[\d.,]*\s*mi)\)', loc_text)
                if dist_match:
                    distance = dist_match.group(1)
                    location = loc_text[:dist_match.start()].strip()
                else:
                    location = loc_text

            # Image
            img_el = card.select_one("img")
            image_url = ""
            if img_el:
                image_url = img_el.get("src", "") or img_el.get("data-src", "")

            # VIN & trim come straight from the structured JSON parsed above.
            vin = (vd.get("vin") or "").strip()
            trim = (vd.get("trim") or "").strip()

            # Get full card text for pattern matching
            card_text = card.get_text(" ", strip=True)
            card_text_lower = card_text.lower()

            # Title type — only specific title-bearing phrases (bare
            # "lemon"/"salvage" matched Cars.com boilerplate)
            title_type = detect_title_type(card_text_lower) or ""

            # Deal rating — fuse renders it in a <fuse-badge> ("Great Deal",
            # "Good Deal", "Fair Price", ...); keep class fallbacks for legacy.
            deal_rating = ""
            for el in card.select("fuse-badge, [class*='deal'], [class*='badge']"):
                txt = el.get_text(strip=True)
                if txt and any(w in txt.lower() for w in
                               ["deal", "fair price", "high price", "overpriced"]):
                    deal_rating = txt
                    break

            # Accident history
            accident_history = ""
            if "no accident" in card_text_lower or "no accidents" in card_text_lower:
                accident_history = "No Accidents"
            elif "accident" in card_text_lower:
                accident_history = "Accident Reported"

            # Owner count — e.g. "1-Owner" or "One-Owner"
            owner_count = ""
            owner_match = re.search(r'(\d+)[- ]?owner', card_text_lower)
            if owner_match:
                owner_count = owner_match.group(1)
            elif "one-owner" in card_text_lower or "one owner" in card_text_lower:
                owner_count = "1"

            # Carfax link — Cars.com often includes "Free CARFAX Report"
            carfax_url = ""
            for link in card.select("a[href*='carfax'], a[href*='CARFAX']"):
                carfax_url = link.get("href", "")
                if carfax_url:
                    break
            if not carfax_url:
                for link in card.select("a"):
                    if "carfax" in (link.get_text(strip=True) or "").lower():
                        carfax_url = link.get("href", "")
                        if carfax_url:
                            break

            seller_type = classify_seller_type(
                seller_name=seller, source="carscom") or ""

            # Sanity backstop: results are queried with list_price_min, so a
            # parsed price below the configured minimum is a mis-parse (almost
            # always a price-drop amount that slipped past the filters above).
            price_val = parse_price(price_str)
            if price_val is not None and price_val < self.min_price:
                logging.debug(
                    f"[Cars.com] Rejecting implausible price '{price_str}' "
                    f"(< min {self.min_price}) for {title}")
                return False

            self.counted_insert(
                car_query=car_query, href=href, image_url=image_url,
                price=price_str, car_name=title, location=location,
                mileage_raw=mileage_str, source=self.SOURCE_NAME,
                seller=seller, distance=distance, title_type=title_type,
                trim=trim, deal_rating=deal_rating,
                accident_history=accident_history,
                owner_count=owner_count, carfax_url=carfax_url,
                seller_type=seller_type, vin=vin,
            )
            return True
        except Exception as e:
            self.count_parse_error()
            logging.warning(f"[Cars.com] Parse error: {e}")
            return False

    def enrich_listings(self, db, limit=60):
        """Visit Cars.com detail pages to extract seller notes and title info."""
        rows = db.get_listings_missing_title_type(source="carscom", limit=limit)
        if not rows:
            self.log("No Cars.com listings need enrichment.")
            return 0

        self.log(f"Enriching {len(rows)} Cars.com listings...")
        enriched = 0

        for row in rows:
            href = row["href"]
            try:
                self.driver.get(href)
                self.inject_stealth()
                self.human_delay(3, 6)

                body_text = self.driver.find_element(By.TAG_NAME, "body").text
                body_lower = body_text.lower()

                details = {}

                # Title type — specific phrases only. "lemon" alone matched
                # Cars.com's "Lemon Law" disclaimer on every page (55 bogus
                # lemon flags); require "lemon law buyback" etc. instead.
                tt = detect_title_type(body_lower)
                if tt:
                    details["title_type"] = tt

                # Accident history
                if "no accident" in body_lower:
                    details["accident_history"] = "No Accidents"
                elif "accident reported" in body_lower or "has been in" in body_lower:
                    details["accident_history"] = "Accident Reported"

                # Owner count
                owner_match = re.search(r'(\d+)[- ]?owner', body_lower)
                if owner_match:
                    details["owner_count"] = owner_match.group(1)
                elif "one-owner" in body_lower or "one owner" in body_lower:
                    details["owner_count"] = "1"

                # Seller notes / description — extract the block after "Seller's notes"
                desc_match = re.search(
                    r"(?:seller.s? notes?|description)(.*?)(?:features|specs|finance|contact|similar|$)",
                    body_text, re.IGNORECASE | re.DOTALL
                )
                if desc_match:
                    desc = desc_match.group(1).strip()[:2000]
                    if len(desc) > 20:
                        details["description"] = desc

                        # Try to extract VIN from description
                        vin = extract_vin(desc)
                        if vin:
                            details["vin"] = vin

                        # Re-check title from the seller-notes block
                        if "title_type" not in details:
                            tt = detect_title_type(desc)
                            if tt:
                                details["title_type"] = tt

                if details:
                    db.update_listing_details(href, **details)
                    enriched += 1
                    tt = details.get("title_type", "—")
                    self.log(f"  Enriched: {row['car_name'][:40]} → title={tt}")
                else:
                    db.mark_enriched(href)

            except Exception as e:
                logging.warning(f"[Cars.com] Enrich error for {href[:60]}: {e}")
                try:
                    db.mark_enriched(href)
                except Exception:
                    pass

            self.human_delay(1, 3)

        self.log(f"Enrichment complete: {enriched}/{len(rows)} listings updated.")
        return enriched
