"""KSL Cars scraper — HTTP-based, no Selenium needed.

KSL Cars (cars.ksl.com) is a Utah-local marketplace. Listings are
server-rendered via Next.js RSC with full JSON embedded in the HTML,
so we can extract structured data with a simple HTTP GET + parse.
"""

import json
import logging
import re
import time
import random

import requests

from scrapers.base import BaseScraper
from parsing import classify_seller_type


class KSLScraper(BaseScraper):
    SOURCE_NAME = "ksl"
    NEEDS_DRIVER = False

    def __init__(self, driver, config, insert_fn, car_list=None):
        super().__init__(driver, config, insert_fn, car_list)
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) "
                "Gecko/20100101 Firefox/128.0"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

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
            logging.debug(f"[KSL] Parse error: {e}")
            return False

    def log(self, msg):
        logging.info(f"[{self.SOURCE_NAME}] {msg}")
