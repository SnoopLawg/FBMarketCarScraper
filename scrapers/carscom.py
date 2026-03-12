"""Cars.com scraper — per-car keyword search for targeted results."""

import re
import logging
from urllib.parse import quote_plus

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class CarsComScraper(BaseScraper):
    SOURCE_NAME = "carscom"

    def scrape(self):
        cc_config = self.config["Sources"].get("carscom", {})
        zip_code = cc_config.get("zip", "84101")
        max_dist = cc_config.get("max_distance", 100)
        max_pages = cc_config.get("max_pages", 3)

        total_found = 0
        total_matched = 0

        for i, car_query in enumerate(self.desired_cars):
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

            for page in range(max_pages):
                url = base_url if page == 0 else f"{base_url}&page={page + 1}"
                self.log(f"  Page {page + 1}...")

                if page > 0:
                    self.human_delay(8, 15)

                try:
                    self.driver.get(url)
                except Exception as e:
                    self.log(f"  Failed to load page {page + 1}: {e}")
                    break

                self.human_delay(4, 8)
                self.scroll_page(count=4)

                try:
                    WebDriverWait(self.driver, 12).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR,
                             "spark-card[data-listing-id], "
                             "[data-listing-id], "
                             ".vehicle-card")
                        )
                    )
                except Exception:
                    self.log(f"  No results on page {page + 1}")
                    break

                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                cards = soup.select("spark-card[data-listing-id]")
                if not cards:
                    cards = [el for el in soup.select("[data-listing-id]")
                             if el.select_one("a[href*='/vehicledetail']")]
                if not cards:
                    cards = soup.select(".vehicle-card")
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

            # Price
            price_str = ""
            for sel in ["span.spark-body-larger", ".primary-price", "[class*='price']"]:
                price_el = card.select_one(sel)
                if price_el:
                    txt = price_el.get_text(strip=True)
                    if "$" in txt and any(c.isdigit() for c in txt):
                        price_str = txt
                        break

            # Mileage
            mileage_str = "N/A"
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

            # Get full card text for pattern matching
            card_text = card.get_text(" ", strip=True)
            card_text_lower = card_text.lower()

            # Title type — search card text for salvage/rebuilt keywords
            title_type = ""
            if "salvage" in card_text_lower:
                title_type = "salvage"
            elif "rebuilt" in card_text_lower:
                title_type = "rebuilt"
            elif "lemon" in card_text_lower:
                title_type = "lemon"
            elif "clean title" in card_text_lower:
                title_type = "clean"

            # Deal rating — Cars.com shows "Great Deal", "Good Deal", etc.
            deal_rating = ""
            for el in card.select("[class*='deal'], [class*='badge'], [class*='price-badge']"):
                txt = el.get_text(strip=True)
                if txt and any(w in txt.lower() for w in ["deal", "price"]):
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

            self.counted_insert(
                car_query=car_query, href=href, image_url=image_url,
                price=price_str, car_name=title, location=location,
                mileage_raw=mileage_str, source=self.SOURCE_NAME,
                seller=seller, distance=distance, title_type=title_type,
                deal_rating=deal_rating, accident_history=accident_history,
                owner_count=owner_count, carfax_url=carfax_url,
            )
            return True
        except Exception as e:
            logging.debug(f"[Cars.com] Parse error: {e}")
            return False
