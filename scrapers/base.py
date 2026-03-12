"""Base scraper with shared scrolling and timing helpers."""

import time
import random
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from config import get_all_search_queries

SCREENSHOTS_DIR = Path(__file__).parent.parent / "screenshots"


class BaseScraper(ABC):
    SOURCE_NAME = ""

    def __init__(self, driver, config, insert_fn):
        self.driver = driver
        self.config = config
        self.insert = insert_fn
        self.desired_cars = get_all_search_queries(config)
        self.min_price = config["MinPrice"]
        self.max_price = config["MaxPrice"]
        self.scroll_count = config.get("ScrollCount", 10)
        self._listing_count = 0

    @abstractmethod
    def scrape(self):
        """Scrape all desired cars from this source."""
        ...

    @property
    def listing_count(self):
        """Number of listings found during this scrape session."""
        return self._listing_count

    def counted_insert(self, **kwargs):
        """Wrapper around insert that counts successful calls."""
        self.insert(**kwargs)
        self._listing_count += 1

    # ── Shared helpers ─────────────────────────────────────────────

    def human_delay(self, min_s=2, max_s=6):
        time.sleep(random.uniform(min_s, max_s))

    def delay_between_searches(self):
        delay = random.uniform(5, 15)
        logging.info(f"[{self.SOURCE_NAME}] Waiting {delay:.1f}s before next search...")
        time.sleep(delay)

    def scroll_page(self, count=None):
        """Randomized scrolling to load dynamic content."""
        n = (count or self.scroll_count) + random.randint(-2, 3)
        for _ in range(max(3, n)):
            if random.random() < 0.6:
                self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
            else:
                px = random.randint(500, 1200)
                self.driver.execute_script(f"window.scrollBy(0, {px})")
            time.sleep(random.uniform(1.5, 4.0))
            if random.random() < 0.15:
                time.sleep(random.uniform(3, 8))

    def inject_stealth(self):
        """Re-inject stealth JS after page navigation."""
        try:
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
        except Exception:
            pass

    def capture_screenshot(self, context="error"):
        """Save a browser screenshot for debugging. Returns the file path or None."""
        try:
            SCREENSHOTS_DIR.mkdir(exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.SOURCE_NAME}_{context}_{ts}.png"
            filepath = SCREENSHOTS_DIR / filename
            self.driver.save_screenshot(str(filepath))
            self.log(f"Screenshot saved: {filepath}")
            return str(filepath)
        except Exception as e:
            logging.warning(f"[{self.SOURCE_NAME}] Screenshot failed: {e}")
            return None

    def log(self, msg):
        logging.info(f"[{self.SOURCE_NAME}] {msg}")
