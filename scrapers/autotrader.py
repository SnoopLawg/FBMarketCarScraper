"""Autotrader scraper — per-car keyword search for targeted results."""

import re
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class AutotraderScraper(BaseScraper):
    SOURCE_NAME = "autotrader"

    def scrape(self):
        at_config = self.config["Sources"].get("autotrader", {})
        zip_code = at_config.get("zip", "84101")
        radius = at_config.get("search_radius", 100)
        max_pages = at_config.get("max_pages", 3)

        total_found = 0
        total_matched = 0

        for i, car_query in enumerate(self.desired_cars):
            self.log(f"Scraping: {car_query}")
            if i > 0:
                self.delay_between_searches()

            # Autotrader supports keyword search via the URL path + params
            # e.g. /cars-for-sale/used-cars/toyota/tacoma/...
            parts = car_query.lower().split()
            make = parts[0] if parts else ""
            model = "-".join(parts[1:]) if len(parts) > 1 else ""
            base_url = (
                f"https://www.autotrader.com/cars-for-sale/used-cars"
                f"/{make}/{model}"
                f"?zip={zip_code}&searchRadius={radius}"
                f"&minPrice={self.min_price}&maxPrice={self.max_price}"
            )

            for page in range(max_pages):
                url = base_url if page == 0 else f"{base_url}&firstRecord={page * 25}"
                self.log(f"  Page {page + 1}...")

                if page > 0:
                    self.human_delay(8, 18)

                try:
                    self.driver.get(url)
                except Exception as e:
                    self.log(f"  Failed to load page {page + 1}: {e}")
                    break

                self.human_delay(5, 10)

                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR,
                             "[data-cmp='inventoryListing'], .inventory-listing")
                        )
                    )
                except Exception:
                    self.log(f"  No results on page {page + 1}")
                    break

                self.scroll_page(count=4)

                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                cards = soup.select("[data-cmp='inventoryListing']")
                if not cards:
                    cards = soup.select(".inventory-listing")
                if not cards:
                    break

                total_found += len(cards)
                for card in cards:
                    if self._process_listing(card, car_query):
                        total_matched += 1

                self.log(f"  Page {page + 1}: {len(cards)} listings")

        self.log(f"Done: {total_found} total listings, {total_matched} inserted")

    def _process_listing(self, card, car_query):
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

            self.counted_insert(
                car_query=car_query, href=href, image_url=image_url,
                price=price_str, car_name=title, location=location,
                mileage_raw=mileage_str, source=self.SOURCE_NAME,
                seller=seller, distance=distance, trim=trim,
                deal_rating=deal_rating, accident_history=accident_history,
                title_type=title_type, owner_count=owner_count,
                carfax_url=carfax_url,
            )
            return True
        except Exception as e:
            logging.debug(f"[Autotrader] Parse error: {e}")
            return False
