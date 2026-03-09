"""Facebook Marketplace scraper."""

import time
import pickle
import logging
from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper

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

        self.insert(
            car_query=car_query, href=full_href, image_url=img_tag["src"],
            price=price, car_name=title, location=city,
            mileage_raw=miles, source=self.SOURCE_NAME,
        )

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
