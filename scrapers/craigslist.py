"""Craigslist scraper — per-car keyword search for targeted results."""

import re
import logging
from urllib.parse import quote_plus

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from parsing import classify_seller_type


class CraigslistScraper(BaseScraper):
    SOURCE_NAME = "craigslist"

    def scrape(self):
        cl_config = self.config["Sources"].get("craigslist", {})
        region = cl_config.get("region", "saltlakecity")
        max_pages = cl_config.get("max_pages", 5)

        total_found = 0
        total_matched = 0

        for i, car_query in enumerate(self.desired_cars):
            self.log(f"Scraping: {car_query}")
            if i > 0:
                self.delay_between_searches()

            # Craigslist supports a query= parameter for keyword search
            base_url = (
                f"https://{region}.craigslist.org/search/cta"
                f"?min_price={self.min_price}&max_price={self.max_price}"
                f"&query={quote_plus(car_query)}"
            )

            for page in range(max_pages):
                page_url = base_url if page == 0 else f"{base_url}#search=1~gallery~{page}"
                self.log(f"  Page {page + 1}...")

                try:
                    self.driver.get(page_url)
                except Exception as e:
                    self.log(f"  Failed to load page {page + 1}: {e}")
                    break

                self.human_delay(3, 6)

                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, ".cl-search-result, .result-row, .gallery-card")
                        )
                    )
                except Exception:
                    self.log(f"  No results on page {page + 1}")
                    break

                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                results = (
                    soup.select(".cl-search-result")
                    or soup.select(".result-row")
                    or soup.select(".gallery-card")
                )

                if not results:
                    break

                total_found += len(results)
                for item in results:
                    if self._process_listing(item, region, car_query):
                        total_matched += 1

                self.log(f"  Page {page + 1}: {len(results)} listings")

                # Check if there are more pages
                next_btn = soup.select_one("button.cl-next-page, .next, a.next")
                if not next_btn or next_btn.get("disabled"):
                    break

                if page < max_pages - 1:
                    self.human_delay(4, 8)

        self.log(f"Done: {total_found} total listings, {total_matched} inserted")

    def _process_listing(self, item, region, car_query):
        """Parse and insert a listing. Returns True if inserted."""
        try:
            # Title and link
            title_el = (
                item.select_one(".posting-title a")
                or item.select_one("a.posting-title")
                or item.select_one(".result-title")
                or item.select_one("a.titlestring")
                or item.select_one("a[href]")
            )
            if not title_el:
                return False
            title = title_el.get_text(strip=True)
            if not title:
                return False

            href = title_el.get("href", "")
            if not href:
                return False
            if not href.startswith("http"):
                href = f"https://{region}.craigslist.org{href}"

            # Price
            price_el = item.select_one(".priceinfo, .result-price, .price")
            price_str = price_el.get_text(strip=True) if price_el else ""

            # Meta contains mileage + location (e.g., "2/19 256k mi POCATELLO")
            mileage_str = "N/A"
            location = ""
            meta_el = item.select_one(".meta")
            if meta_el:
                meta_text = meta_el.get_text(" ", strip=True)
                mi_match = re.search(r'([\d,]+k?)\s*mi', meta_text, re.IGNORECASE)
                if mi_match:
                    mileage_str = mi_match.group(0)
                loc_text = re.sub(r'\d+/\d+', '', meta_text)
                loc_text = re.sub(r'[\d,]+k?\s*mi', '', loc_text, flags=re.IGNORECASE)
                loc_text = loc_text.strip()
                if loc_text:
                    location = loc_text

            if not location:
                loc_el = item.select_one(".result-hood, .location")
                location = loc_el.get_text(strip=True).strip("() ") if loc_el else ""

            # Image
            img_el = item.select_one("img")
            image_url = ""
            if img_el:
                image_url = img_el.get("src", "") or img_el.get("data-src", "")

            # Title type — Craigslist sometimes shows in listing text
            title_type = ""
            item_text_lower = item.get_text(" ", strip=True).lower()
            if "salvage" in item_text_lower:
                title_type = "salvage"
            elif "rebuilt" in item_text_lower:
                title_type = "rebuilt"
            elif "lemon" in item_text_lower:
                title_type = "lemon"
            elif "clean title" in item_text_lower:
                title_type = "clean"

            seller_type = classify_seller_type(href=href, source="craigslist")

            self.counted_insert(
                car_query=car_query, href=href, image_url=image_url,
                price=price_str, car_name=title, location=location,
                mileage_raw=mileage_str, source=self.SOURCE_NAME,
                title_type=title_type, seller_type=seller_type or "",
            )
            return True
        except Exception as e:
            logging.debug(f"[Craigslist] Parse error: {e}")
            return False
