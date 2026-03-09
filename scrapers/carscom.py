"""Cars.com scraper — broad search with local car matching."""

import re
import logging

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

        # Build match patterns from desired cars
        self._car_patterns = {}
        for car in self.desired_cars:
            words = car.lower().split()
            self._car_patterns[car] = words

        # Broad search: all used cars in price range, no keyword
        base_url = (
            f"https://www.cars.com/shopping/results/"
            f"?zip={zip_code}&maximum_distance={max_dist}"
            f"&list_price_max={self.max_price}&list_price_min={self.min_price}"
            f"&stock_type=used&sort=best_match_desc"
        )

        total_found = 0
        total_matched = 0

        for page in range(max_pages):
            # Cars.com uses page_size and page params
            url = base_url if page == 0 else f"{base_url}&page={page + 1}"
            self.log(f"Loading page {page + 1}...")

            if page > 0:
                self.human_delay(8, 15)

            try:
                self.driver.get(url)
            except Exception as e:
                self.log(f"Failed to load page {page + 1}: {e}")
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
                self.log(f"No results on page {page + 1}")
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
                if self._process_listing(card):
                    total_matched += 1

            self.log(f"Page {page + 1}: {len(cards)} listings")

        self.log(f"Done: {total_found} total listings, {total_matched} matched desired cars")

    def _match_car(self, title):
        """Match a listing title against desired cars. Returns car_query or None."""
        title_lower = title.lower()
        for car_query, words in self._car_patterns.items():
            if all(w in title_lower for w in words):
                return car_query
        return None

    def _process_listing(self, card):
        """Parse and insert a listing. Returns True if it matched a desired car."""
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

            # Match against desired cars
            car_query = self._match_car(title)
            if not car_query:
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

            self.insert(
                car_query=car_query, href=href, image_url=image_url,
                price=price_str, car_name=title, location=location,
                mileage_raw=mileage_str, source=self.SOURCE_NAME,
                seller=seller, distance=distance,
            )
            return True
        except Exception as e:
            logging.debug(f"[Cars.com] Parse error: {e}")
            return False
