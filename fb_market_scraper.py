import time
import re
import sys
import json
import sqlite3
import logging
import webbrowser
import tkinter as tk
from tkinter import messagebox
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

SCRIPT_DIR = Path(__file__).parent


class FacebookMarketplaceScraper:
    def __init__(self):
        logging.info("Initializing FacebookMarketplaceScraper...")
        self.config = self._load_config()
        self.desired_cars = self.config["DesiredCar"]
        self.min_mileage = self.config.get("MileageMin")
        self.max_mileage = self.config.get("MileageMax")
        self.city_id = self.config["CityID"]
        self.min_price = self.config["MinPrice"]
        self.max_price = self.config["MaxPrice"]
        self.price_threshold = self.config["PriceThreshold"]
        self.location_filter = self.config.get("LocationFilter", "UT")
        self.scroll_count = self.config.get("ScrollCount", 10)
        self.passive_mode = self.config.get("Passive")

        self.viewed = self._load_set_from_file(SCRIPT_DIR / "viewed_listings.txt")
        self.deleted = self._load_set_from_file(SCRIPT_DIR / "deleted_listings.txt")
        self.favorites = self._load_set_from_file(SCRIPT_DIR / "favorite_listings.txt")

        self.conn = None
        self.cur = None
        self.driver = None
        self.deals = []
        self.current_deal_index = 0
        self.current_listing_href = None

    # ── Config & file helpers ──────────────────────────────────────────

    @staticmethod
    def _load_config():
        config_path = SCRIPT_DIR / "Config.json"
        if not config_path.exists():
            raise FileNotFoundError(f"Config.json not found at {config_path}")
        with open(config_path, "r") as f:
            return json.load(f)

    @staticmethod
    def _load_set_from_file(filepath):
        try:
            with open(filepath, "r") as f:
                return set(line.strip() for line in f if line.strip())
        except FileNotFoundError:
            return set()

    @staticmethod
    def _append_to_file(filepath, value):
        with open(filepath, "a") as f:
            f.write(value + "\n")

    # ── Database ───────────────────────────────────────────────────────

    def _open_db(self):
        db_path = SCRIPT_DIR / "marketplace_listings.db"
        self.conn = sqlite3.connect(str(db_path))
        self.cur = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        self.cur.executescript("""
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                href TEXT UNIQUE,
                image_url TEXT,
                price REAL,
                car_name TEXT,
                car_query TEXT,
                location TEXT,
                mileage REAL,
                year INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_listings_car_query ON listings(car_query);
            CREATE INDEX IF NOT EXISTS idx_listings_href ON listings(href);

            CREATE TABLE IF NOT EXISTS average_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                car_query TEXT,
                year INTEGER,
                avg_lower_mileage_price REAL,
                avg_higher_mileage_price REAL,
                UNIQUE(car_query, year)
            );
        """)
        self.conn.commit()

    def _close_db(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cur = None

    def _insert_listing(self, car_query, href, image_url, price, car_name, location, mileage_raw):
        if href in self.deleted:
            return

        price_val = self._parse_price(price)
        mileage_val = self._parse_mileage(mileage_raw)
        year_val = self._extract_year(car_name)

        try:
            self.cur.execute("""
                INSERT OR IGNORE INTO listings
                    (href, image_url, price, car_name, car_query, location, mileage, year)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (href, image_url, price_val, car_name, car_query, location, mileage_val, year_val))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB insert error: {e}")

    # ── Parsing helpers ────────────────────────────────────────────────

    @staticmethod
    def _parse_price(price_str):
        if not price_str or price_str == "Sold":
            return None
        try:
            cleaned = price_str.replace("$", "").replace(",", "").strip()
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def _parse_mileage(mileage_str):
        if not mileage_str or mileage_str == "N/A":
            return None
        match = re.search(r"(\d+[\d,]*\.?\d*)\s*[Kk]?", mileage_str)
        if not match:
            return None
        try:
            num = float(match.group(1).replace(",", ""))
            # If the value is small (e.g. "120K" parsed as 120), it's in thousands
            if num < 1000:
                num *= 1000
            return num
        except ValueError:
            return None

    @staticmethod
    def _extract_year(car_name):
        match = re.search(r"\b(19|20)\d{2}\b", car_name)
        return int(match.group()) if match else None

    # ── Selenium driver ────────────────────────────────────────────────

    def _start_driver(self):
        if self.driver is None:
            options = Options()
            self.driver = webdriver.Firefox(options=options)

    def _quit_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    # ── URL builder ────────────────────────────────────────────────────

    def _get_url(self, car):
        if not self.passive_mode:
            return (
                f"https://www.facebook.com/marketplace/{self.city_id}/search"
                f"?minPrice={self.min_price}&maxPrice={self.max_price}"
                f"&query={car}&exact=false"
            )
        else:
            return (
                f"https://www.facebook.com/marketplace/{self.city_id}/vehicles/"
                f"?minPrice={self.min_price}&maxPrice={self.max_price}"
                f"&maxMileage={self.max_mileage}&topLevelVehicleType=car_truck&exact=false"
            )

    # ── Scraping ───────────────────────────────────────────────────────

    def scrape_data(self):
        logging.info("Starting data scraping...")
        self._start_driver()
        self._open_db()

        try:
            time.sleep(5)
            for car_query in self.desired_cars:
                logging.info(f"Scraping listings for: {car_query}")
                self.driver.get(self._get_url(car_query))
                self.driver.implicitly_wait(15)

                # Scroll to load dynamic content
                for _ in range(self.scroll_count):
                    self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
                    time.sleep(2)

                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")
                listings = soup.find_all(class_="x3ct3a4")

                for item in listings:
                    self._process_listing_element(item, car_query)

        except Exception as e:
            logging.error(f"Scraping error: {e}")
        finally:
            self._quit_driver()
            logging.info("Data scraping completed.")

    def _process_listing_element(self, item, car_query):
        price_divs = item.find_all("div", class_="x1gslohp xkh6y0r")

        if len(price_divs) == 3:
            price_str = price_divs[0].text.strip()
            title = price_divs[1].text.strip()
            city = price_divs[2].text.strip()
            miles = "N/A"
        elif len(price_divs) == 4:
            price_str = price_divs[0].text.strip()
            title = price_divs[1].text.strip()
            city = price_divs[2].text.strip()
            miles = price_divs[3].text.strip()
        else:
            return

        # Parse price from the text (may have "$1,234" or "$1,234$2,000")
        price_parts = price_str.split("$")[1:]
        if not price_parts:
            return
        price = price_parts[0]  # Use the first (current) price

        # Get link and image
        link_tag = item.find("a")
        img_tag = item.find("img", {"src": True})
        if not link_tag or not img_tag:
            return

        href = link_tag.get("href")
        if not href:
            return

        self._insert_listing(
            car_query=car_query,
            href=href,
            image_url=img_tag["src"],
            price=price,
            car_name=title,
            location=city,
            mileage_raw=miles,
        )

    # ── Database cleanup ───────────────────────────────────────────────

    def clean_listings(self):
        logging.info("Cleaning listings that don't match desired car names...")
        for car_query in self.desired_cars:
            self.cur.execute(
                "SELECT id, car_name FROM listings WHERE car_query = ?",
                (car_query,)
            )
            for row_id, car_name in self.cur.fetchall():
                if car_query.lower() not in car_name.lower():
                    self.cur.execute("DELETE FROM listings WHERE id = ?", (row_id,))
                    logging.info(f"Removed mismatched listing id={row_id}: '{car_name}'")
        self.conn.commit()

    # ── Average price calculation ──────────────────────────────────────

    def calculate_averages(self):
        logging.info("Calculating average prices...")
        mileage_threshold = self.max_mileage if self.max_mileage else 150000

        for car_query in self.desired_cars:
            self.cur.execute(
                "SELECT price, mileage, year FROM listings WHERE car_query = ? AND price IS NOT NULL AND year IS NOT NULL",
                (car_query,)
            )
            rows = self.cur.fetchall()

            # Group by year
            year_data = {}
            for price, mileage, year in rows:
                if year not in year_data:
                    year_data[year] = []
                year_data[year].append((price, mileage or 0))

            for year, data in year_data.items():
                lower = [p for p, m in data if m <= mileage_threshold]
                higher = [p for p, m in data if m > mileage_threshold]

                avg_lower = round(sum(lower) / len(lower)) if lower else 0
                avg_higher = round(sum(higher) / len(higher)) if higher else 0

                self.cur.execute("""
                    INSERT OR REPLACE INTO average_prices
                        (car_query, year, avg_lower_mileage_price, avg_higher_mileage_price)
                    VALUES (?, ?, ?, ?)
                """, (car_query, year, avg_lower, avg_higher))

        self.conn.commit()

    # ── Deal assessment ────────────────────────────────────────────────

    def find_deals(self):
        logging.info("Assessing deals...")
        self.deals = []
        mileage_threshold = self.max_mileage if self.max_mileage else 150000

        for car_query in self.desired_cars:
            # Load averages for this car
            self.cur.execute(
                "SELECT year, avg_lower_mileage_price, avg_higher_mileage_price FROM average_prices WHERE car_query = ?",
                (car_query,)
            )
            avg_by_year = {row[0]: (row[1], row[2]) for row in self.cur.fetchall()}

            # Check each listing
            self.cur.execute(
                "SELECT href, price, mileage, year, location FROM listings WHERE car_query = ? AND price IS NOT NULL",
                (car_query,)
            )
            for href, price, mileage, year, location in self.cur.fetchall():
                if not year or year not in avg_by_year:
                    continue
                if self.location_filter and self.location_filter not in (location or ""):
                    continue

                mileage = mileage or 0
                avg_lower, avg_higher = avg_by_year[year]

                is_deal = False
                if mileage <= mileage_threshold:
                    # Low mileage: is it priced well below average?
                    if avg_lower > 0 and price < (avg_lower - self.price_threshold):
                        # Check min mileage filter
                        if self.min_mileage is None or mileage > self.min_mileage:
                            is_deal = True
                else:
                    # High mileage: is it priced well below average?
                    if avg_higher > 0 and price < (avg_higher - self.price_threshold):
                        # Check max mileage filter
                        if self.max_mileage is None or mileage < self.max_mileage:
                            is_deal = True

                if is_deal:
                    full_url = f"https://www.facebook.com{href}"
                    self.deals.append(full_url)

        logging.info(f"Found {len(self.deals)} deals.")
        self.current_deal_index = 0

    # ── GUI ────────────────────────────────────────────────────────────

    def gui_navigator(self):
        if not self.deals:
            logging.info("No deals found. Nothing to navigate.")
            return

        self._start_driver()

        self.root = tk.Tk()
        self.root.title("Deal Navigator")
        self.root.attributes("-topmost", True)

        frame = tk.Frame(self.root)
        frame.pack(pady=20)

        tk.Button(frame, text="Quit", command=self._quit_gui).pack(side=tk.LEFT, padx=10)
        tk.Button(frame, text="Delete Listing", command=self._delete_current).pack(side=tk.LEFT, padx=10)
        tk.Button(frame, text="Favorite", command=self._favorite_current).pack(side=tk.LEFT, padx=10)
        tk.Button(frame, text="Next", command=self._open_next_deal).pack(side=tk.RIGHT, padx=10)

        self.deal_label = tk.Label(self.root, text=f"0 / {len(self.deals)} deals")
        self.deal_label.pack(pady=5)

        self.root.mainloop()

    def _open_next_deal(self):
        while self.current_deal_index < len(self.deals):
            url = self.deals[self.current_deal_index]
            if url not in self.viewed:
                self.current_listing_href = url
                self.viewed.add(url)
                self._append_to_file(SCRIPT_DIR / "viewed_listings.txt", url)

                # Close extra tabs
                if len(self.driver.window_handles) > 1:
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])

                self.driver.execute_script(f"window.open('{url}', '_blank')")
                self.deal_label.config(
                    text=f"{self.current_deal_index + 1} / {len(self.deals)} deals"
                )
                self.current_deal_index += 1
                return

            self.current_deal_index += 1

        messagebox.showinfo("Info", "No more deals to view.")

    def _delete_current(self):
        if not self.current_listing_href:
            messagebox.showwarning("Warning", "No listing selected.")
            return

        # Remove from DB
        relative_href = self.current_listing_href.replace("https://www.facebook.com", "")
        self.cur.execute("DELETE FROM listings WHERE href = ?", (relative_href,))
        self.conn.commit()

        # Track deletion
        self.deleted.add(self.current_listing_href)
        self._append_to_file(SCRIPT_DIR / "deleted_listings.txt", self.current_listing_href)
        logging.info(f"Deleted listing: {self.current_listing_href}")

        self._open_next_deal()

    def _favorite_current(self):
        if not self.current_listing_href:
            messagebox.showwarning("Warning", "No listing selected.")
            return

        if self.current_listing_href not in self.favorites:
            self.favorites.add(self.current_listing_href)
            self._append_to_file(SCRIPT_DIR / "favorite_listings.txt", self.current_listing_href)
        messagebox.showinfo("Info", "Listing added to favorites!")

    def _quit_gui(self):
        self._quit_driver()
        self._close_db()
        self.root.quit()
        self.root.destroy()

    def open_favorites(self):
        for url in self.favorites:
            webbrowser.open(url)

    # ── Main run ───────────────────────────────────────────────────────

    def run(self):
        try:
            # Phase 1: Scrape
            self.scrape_data()

            # Phase 2: Analyze
            self._open_db()
            self.clean_listings()
            self.calculate_averages()

            # Phase 3: Find deals and navigate
            self.find_deals()
            self.gui_navigator()

        except KeyboardInterrupt:
            logging.info("Interrupted by user.")
        except Exception as e:
            logging.error(f"Fatal error: {e}")
        finally:
            self._quit_driver()
            self._close_db()


if __name__ == "__main__":
    scraper = FacebookMarketplaceScraper()
    scraper.run()
