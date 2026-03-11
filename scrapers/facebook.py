"""Facebook Marketplace scraper."""

import time
import pickle
import logging
from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from vin import extract_vin

SCRIPT_DIR = Path(__file__).parent.parent
COOKIE_FILE = SCRIPT_DIR / "fb_cookies.pkl"


class FacebookScraper(BaseScraper):
    SOURCE_NAME = "facebook"

    def scrape(self):
        fb_config = self.config["Sources"].get("facebook", {})
        city_id = fb_config.get("CityID", "")

        if not self._ensure_logged_in():
            self.log("Could not log in. Skipping.")
            return

        for i, car_query in enumerate(self.desired_cars):
            self.log(f"Scraping: {car_query}")
            if i > 0:
                self.delay_between_searches()

            url = self._build_url(city_id, car_query)
            self.driver.get(url)
            self.inject_stealth()
            self.human_delay(3, 7)
            self.scroll_page()

            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            listings = soup.find_all(class_="x3ct3a4")

            for item in listings:
                self._process_listing(item, car_query)

    def _build_url(self, city_id, car_query):
        passive = self.config.get("Passive")
        if not passive:
            return (
                f"https://www.facebook.com/marketplace/{city_id}/search"
                f"?minPrice={self.min_price}&maxPrice={self.max_price}"
                f"&query={car_query}&exact=false"
            )
        else:
            max_mileage = self.config.get("MileageMax", 150000)
            return (
                f"https://www.facebook.com/marketplace/{city_id}/vehicles/"
                f"?minPrice={self.min_price}&maxPrice={self.max_price}"
                f"&maxMileage={max_mileage}&topLevelVehicleType=car_truck&exact=false"
            )

    def _process_listing(self, item, car_query):
        price_divs = item.find_all("div", class_="x1gslohp xkh6y0r")

        if len(price_divs) == 3:
            price_str, title, city, miles = (
                price_divs[0].text.strip(), price_divs[1].text.strip(),
                price_divs[2].text.strip(), "N/A"
            )
        elif len(price_divs) == 4:
            price_str, title, city, miles = (
                price_divs[0].text.strip(), price_divs[1].text.strip(),
                price_divs[2].text.strip(), price_divs[3].text.strip()
            )
        else:
            return

        price_parts = price_str.split("$")[1:]
        if not price_parts:
            return
        price = price_parts[0]

        link_tag = item.find("a")
        img_tag = item.find("img", {"src": True})
        if not link_tag or not img_tag:
            return

        href = link_tag.get("href")
        if not href:
            return

        full_href = f"https://www.facebook.com{href}" if not href.startswith("http") else href

        # Title type — Facebook occasionally includes it in listing text
        title_type = ""
        full_text = f"{title} {city} {miles}".lower()
        if "salvage" in full_text:
            title_type = "salvage"
        elif "rebuilt" in full_text:
            title_type = "rebuilt"
        elif "clean title" in full_text:
            title_type = "clean"

        self.insert(
            car_query=car_query, href=full_href, image_url=img_tag["src"],
            price=price, car_name=title, location=city,
            mileage_raw=miles, source=self.SOURCE_NAME,
            title_type=title_type,
        )

    # ── Detail page enrichment ──────────────────────────────────────

    def enrich_listings(self, db, limit=40):
        """Visit individual listing pages to extract title type and details.

        Call this AFTER the main scrape.  Visits up to `limit` listings
        that are missing title_type, with human-like delays between visits.
        """
        if not self._ensure_logged_in():
            self.log("Cannot enrich — not logged in.")
            return 0

        rows = db.get_listings_missing_title_type(source="facebook", limit=limit)
        if not rows:
            self.log("No listings need enrichment.")
            return 0

        self.log(f"Enriching {len(rows)} listings with detail page data...")
        enriched = 0

        for row in rows:
            href = row["href"]
            try:
                self.driver.get(href)
                self.inject_stealth()
                self.human_delay(2, 5)

                # Click all "See more" buttons to expand hidden description text
                self._click_see_more()
                self.human_delay(0.5, 1.5)

                page_source = self.driver.page_source
                page_text = page_source.lower()
                details = self._extract_detail_info(page_text)

                # Capture visible description text for future re-parsing
                description = self._extract_description(page_source)
                if description:
                    details["description"] = description

                    # Try to extract VIN from the description
                    vin = extract_vin(description)
                    if vin:
                        details["vin"] = vin

                if details:
                    db.update_listing_details(href, **details)
                    enriched += 1
                    tt = details.get("title_type", "—")
                    vin_str = details.get("vin", "")
                    self.log(f"  Enriched: {row['car_name'][:40]} → title={tt}"
                             f"{' VIN=' + vin_str if vin_str else ''}")
                else:
                    # Mark as 'unknown' so we don't re-visit
                    db.update_title_type(href, "unknown")

            except Exception as e:
                logging.warning(f"[Facebook] Enrich error for {href[:60]}: {e}")

            # Human-like delay between pages
            self.human_delay(1, 3)

        self.log(f"Enrichment complete: {enriched}/{len(rows)} listings updated.")
        return enriched

    def _click_see_more(self):
        """Click all 'See more' / 'See More' links on the page to expand text."""
        from selenium.webdriver.common.by import By

        try:
            # Facebook uses various elements for "See more" — try multiple selectors
            see_more_selectors = [
                "//div[contains(text(), 'See more')]",
                "//span[contains(text(), 'See more')]",
                "//div[contains(text(), 'See More')]",
                "//span[contains(text(), 'See More')]",
                "//div[@role='button' and contains(text(), 'See')]",
            ]
            for xpath in see_more_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    for el in elements[:3]:  # Click up to 3
                        try:
                            el.click()
                            time.sleep(0.3)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass  # Don't let this break enrichment

    def _extract_description(self, page_source):
        """Extract the visible text from a FB listing detail page.

        Stores a clean version of the seller's description and vehicle
        details so we can re-parse later without re-visiting the page.
        """
        try:
            soup = BeautifulSoup(page_source, "html.parser")

            # Remove script/style noise
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()

            # Get all visible text
            text = soup.get_text(separator="\n", strip=True)

            # Trim to a reasonable size (descriptions are rarely > 5k chars)
            # but keep enough to be useful for re-parsing
            if len(text) > 8000:
                text = text[:8000]

            return text if len(text) > 50 else None
        except Exception:
            return None

    def _extract_detail_info(self, page_text):
        """Extract title type, condition, and other info from a FB listing detail page."""
        info = {}
        text = page_text.lower()

        # ── Title type detection ─────────────────────────────────
        # Facebook shows title status in vehicle details or description
        # Check structured patterns first (more reliable), then broad text
        title_patterns = [
            # Structured: ">clean title<" or ">salvage title<"
            (">salvage title<", "salvage"),
            (">rebuilt title<", "rebuilt"),
            (">clean title<", "clean"),
            ('"salvage title"', "salvage"),
            ('"rebuilt title"', "rebuilt"),
            ('"clean title"', "clean"),
            # Common seller descriptions
            ("salvage title", "salvage"),
            ("rebuilt title", "rebuilt"),
            ("branded title", "rebuilt"),
            ("clean title", "clean"),
        ]
        for pattern, ttype in title_patterns:
            if pattern in text:
                info["title_type"] = ttype
                break

        # Broader fallback — "salvage" alone is a strong signal on a car listing
        if "title_type" not in info:
            if "salvage" in text and ("title" in text or "vehicle" in text):
                info["title_type"] = "salvage"
            elif "rebuilt" in text and "title" in text:
                info["title_type"] = "rebuilt"
            elif "lemon" in text and ("title" in text or "law" in text):
                info["title_type"] = "lemon"

        # ── Accident history ─────────────────────────────────────
        if "no accident" in text or "no accidents" in text or "0 accidents" in text:
            info["accident_history"] = "No Accidents"
        elif ("1 accident" in text or "accident reported" in text
              or "accidents reported" in text):
            info["accident_history"] = "Accident Reported"

        # ── Condition ────────────────────────────────────────────
        for cond in ["excellent", "like new", "good", "fair", "poor"]:
            if f">condition<" in text or f"condition" in text:
                # Look for "Condition: Good" or ">Good<" near condition context
                if f">{cond}<" in text or f'"{cond}"' in text:
                    info["condition"] = cond.title()
                    break

        # ── Deal rating (from third-party badges on listing) ─────
        if "great deal" in text:
            info["deal_rating"] = "Great Deal"
        elif "good deal" in text:
            info["deal_rating"] = "Good Deal"
        elif "fair deal" in text:
            info["deal_rating"] = "Fair Deal"

        return info

    # ── Login / cookie management ─────────────────────────────────

    def _ensure_logged_in(self):
        self.log("Checking login status...")
        try:
            if self._load_cookies():
                self.log("Logged in via cookies.")
                return True

            self.driver.get("https://www.facebook.com/")
            self.inject_stealth()
            time.sleep(2)

            if self._is_logged_in():
                self._save_cookies()
                return True

            self.log("Please log in manually in the browser...")
            print("\n" + "=" * 50)
            print("  Please log into Facebook in the browser window.")
            print("  Waiting for login (2 min timeout)...")
            print("=" * 50 + "\n")

            start = time.time()
            while not self._is_logged_in():
                if time.time() - start > 120:
                    self.log("Login timeout.")
                    return False
                time.sleep(3)

            self._save_cookies()
            return True
        except Exception as e:
            logging.error(f"[Facebook] Login error: {e}")
            return False

    def _is_logged_in(self):
        try:
            page = self.driver.page_source.lower()
            if 'id="loginbutton"' in page or 'name="email"' in page:
                return False
            if "/login" in self.driver.current_url:
                return False
            return True
        except Exception:
            return False

    def _save_cookies(self):
        with open(COOKIE_FILE, "wb") as f:
            pickle.dump(self.driver.get_cookies(), f)

    def _load_cookies(self):
        if not COOKIE_FILE.exists():
            return False
        try:
            with open(COOKIE_FILE, "rb") as f:
                cookies = pickle.load(f)
            self.driver.get("https://www.facebook.com/")
            self.inject_stealth()
            for cookie in cookies:
                for key in ["sameSite", "expiry"]:
                    cookie.pop(key, None)
                try:
                    self.driver.add_cookie(cookie)
                except Exception:
                    pass
            self.driver.refresh()
            time.sleep(3)
            return self._is_logged_in()
        except Exception as e:
            logging.warning(f"[Facebook] Cookie load failed: {e}")
            return False
