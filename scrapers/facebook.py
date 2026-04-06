"""Facebook Marketplace scraper."""

import json
import os
import random
import re
import time
import pickle
import logging
from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from vin import extract_vin

SCRIPT_DIR = Path(__file__).parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR", SCRIPT_DIR))
COOKIE_FILE = DATA_DIR / "fb_cookies.pkl"


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

        self.counted_insert(
            car_query=car_query, href=full_href, image_url=img_tag["src"],
            price=price, car_name=title, location=city,
            mileage_raw=miles, source=self.SOURCE_NAME,
            title_type=title_type,
        )

    # ── Detail page enrichment ──────────────────────────────────────

    def enrich_listings(self, db, limit=40):
        """Visit individual listing pages to extract title type and details.

        Works with OR without Facebook login.  If the driver already has
        a logged-in session (e.g. from the main scrape), we use it for
        higher rate limits.  If not, we try auto-login, then fall back
        to logged-out enrichment which still works but gets rate-limited
        faster.
        """
        rows = db.get_listings_missing_title_type(source="facebook", limit=limit)
        if not rows:
            self.log("No listings need enrichment.")
            return 0

        # Try to use existing login or auto-login for better rate limits.
        # If login fails, enrichment still works logged-out.
        logged_in = self._load_cookies()
        if not logged_in:
            logged_in = self._auto_login()
            if logged_in:
                self._save_cookies()
        self.log(f"Enriching {len(rows)} listings "
                 f"({'logged in' if logged_in else 'no login'})...")
        enriched = 0
        consecutive_blocked = 0

        for row in rows:
            href = row["href"]
            try:
                self.driver.get(href)
                self.inject_stealth()
                self.human_delay(2, 5)

                # Close login modal if it appears
                self._dismiss_login_modal()

                # Expand the seller description via "See more"
                self._click_see_more()
                self.human_delay(0.5, 1.5)

                page_source = self.driver.page_source
                page_text = page_source.lower()

                # Validate we landed on a listing page
                cur_url = self.driver.current_url
                if "marketplace/item" not in cur_url or "directory" in cur_url:
                    consecutive_blocked += 1
                    if consecutive_blocked >= 5:
                        self.log(f"  Rate limited by Facebook ({consecutive_blocked} "
                                 f"consecutive blocks). Stopping early.")
                        break
                    self.log(f"  Skipped (blocked): {href[:60]}")
                    self.human_delay(3, 6)
                    continue

                # Reset block counter on successful page load
                consecutive_blocked = 0

                details = self._extract_detail_info(page_text)

                # Capture visible description text for future re-parsing
                description = self._extract_description(page_source)
                if description:
                    details["description"] = description

                    # Try to extract VIN from the description
                    vin = extract_vin(description)
                    if vin:
                        details["vin"] = vin

                    # If no title_type from HTML patterns, check the
                    # extracted description text (catches titles mentioned
                    # in seller descriptions behind "See more")
                    if "title_type" not in details:
                        desc_lower = description.lower()
                        if "salvage title" in desc_lower:
                            details["title_type"] = "salvage"
                        elif "rebuilt title" in desc_lower:
                            details["title_type"] = "rebuilt"
                        elif "branded title" in desc_lower:
                            details["title_type"] = "rebuilt"
                        elif "lemon" in desc_lower and "title" in desc_lower:
                            details["title_type"] = "lemon"
                        elif "clean title" in desc_lower:
                            details["title_type"] = "clean"

                # Extract all listing images from detail page
                image_urls = self._extract_images(page_source)
                if image_urls:
                    details["image_urls"] = json.dumps(image_urls)

                if details:
                    db.update_listing_details(href, **details)
                    enriched += 1
                    tt = details.get("title_type", "—")
                    vin_str = details.get("vin", "")
                    self.log(f"  Enriched: {row['car_name'][:40]} → title={tt}"
                             f"{' VIN=' + vin_str if vin_str else ''}")
                else:
                    # Mark as attempted so we don't re-visit
                    db.mark_enriched(href)

            except Exception as e:
                logging.warning(f"[Facebook] Enrich error for {href[:60]}: {e}")
                try:
                    db.mark_enriched(href)
                except Exception:
                    pass

            # Human-like delay between pages
            self.human_delay(1, 3)

        self.log(f"Enrichment complete: {enriched}/{len(rows)} listings updated.")
        return enriched

    def _dismiss_login_modal(self):
        """Close the Facebook login modal that appears on logged-out pages."""
        try:
            self.driver.execute_script("""
                // Try multiple approaches to close login modals
                // 1. Dialog close button
                const dialog = document.querySelector('[role="dialog"]');
                if (dialog) {
                    const close = dialog.querySelector(
                        '[aria-label="Close"], [aria-label="close"]');
                    if (close) { close.click(); return; }
                    // Try any button in the dialog
                    const btn = dialog.querySelector('button');
                    if (btn) { btn.click(); return; }
                }
                // 2. Bottom banner close
                const bannerClose = document.querySelector(
                    'div[data-nosnippet] button[aria-label="Close"]');
                if (bannerClose) bannerClose.click();
            """)
            time.sleep(0.5)
        except Exception:
            pass

    def _click_see_more(self):
        """Click the 'See more' link in the seller description to expand it.

        On logged-out pages, 'See more' is a <span> with cursor:pointer,
        not a <button>.  We click ALL visible "See more" elements in the
        main content area to ensure the description expands.
        """
        try:
            clicked = self.driver.execute_script("""
                let clicked = 0;
                const candidates = [];
                document.querySelectorAll('div, span, a, button').forEach(el => {
                    const text = el.textContent.trim();
                    if (text === 'See more' || text === 'See More') {
                        candidates.push(el);
                    }
                });

                // Click ALL "See more" elements in the main content area.
                // The sidebar nav ones are on the left (x < 350), the
                // description one is on the right.
                for (const el of candidates) {
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0) {
                        try {
                            el.click();
                            clicked++;
                        } catch(e) {}
                    }
                }
                return clicked;
            """)
            if clicked:
                time.sleep(1)

            # Fallback: use Selenium to find and click span elements
            if not clicked:
                from selenium.webdriver.common.by import By
                spans = self.driver.find_elements(
                    By.XPATH, "//span[text()='See more'] | //span[text()='See More']")
                for span in spans:
                    try:
                        if span.is_displayed():
                            self.driver.execute_script(
                                "arguments[0].click()", span)
                            time.sleep(0.5)
                    except Exception:
                        pass
        except Exception:
            pass

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

            # Detect login wall / non-listing pages.
            # Must check multiple markers together — "Create new account"
            # appears in the footer of ALL FB pages, even when logged in.
            is_login_wall = (
                "Log Into Facebook" in text
                or "Explore the things\nyou love" in text
                or ("Create new account" in text
                    and "About this vehicle" not in text
                    and "Seller" not in text)
                or "This content isn't available" in text
            )
            if is_login_wall:
                return None

            return text if len(text) > 50 else None
        except Exception:
            return None

    def _extract_images(self, page_source):
        """Extract all listing images from a FB detail page.

        Returns a deduplicated list of image URLs (up to 10).
        Filters for FB CDN images (scontent) and skips tiny icons/avatars.
        """
        try:
            from html import unescape
            # Match scontent URLs including &amp; sequences (HTML-encoded &)
            # so we capture the full query string with auth tokens
            urls = re.findall(
                r'https://scontent[^"\'\s]+\.(?:jpg|jpeg|png|webp)(?:[^"\'\s]*)',
                page_source, re.I
            )
            # Decode HTML entities (&amp; → &) in captured URLs
            urls = [unescape(u) for u in urls]
            # Deduplicate while preserving order
            seen = set()
            unique = []
            for url in urls:
                # Normalize: strip query params after the extension for dedup
                key = url.split("?")[0]
                if key not in seen:
                    seen.add(key)
                    unique.append(url)
            # Skip very short URLs (likely tracking pixels)
            unique = [u for u in unique if len(u) > 80]
            return unique[:10]
        except Exception:
            return []

    def _extract_detail_info(self, page_text):
        """Extract title type, condition, and other info from a FB listing detail page."""
        info = {}
        text = page_text.lower()

        # Detect login wall / non-listing pages — return empty to avoid
        # false keyword matches in FB boilerplate HTML
        if ("create new account" in text and "log in" in text
                and "marketplace" not in text[:500]):
            return info

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

        # Override: if structured field says "clean" but description mentions
        # rebuilt/salvage, trust the description — sellers sometimes
        # misrepresent the structured title field
        if info.get("title_type") == "clean":
            # Look in seller description area (after the structured fields)
            seller_idx = text.find("seller")
            desc_text = text[seller_idx:] if seller_idx > 0 else ""
            if desc_text:
                if "rebuilt title" in desc_text or ("rebuilt" in desc_text and "title" in desc_text):
                    info["title_type"] = "rebuilt"
                elif "salvage title" in desc_text or ("salvage" in desc_text and "title" in desc_text):
                    info["title_type"] = "salvage"

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

        # ── Seller type ──────────────────────────────────────
        if "professional seller" in text or ">dealership<" in text:
            info["seller_type"] = "dealer"

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

            # Auto-login with credentials from environment
            if self._auto_login():
                self._save_cookies()
                return True

            self.log("Auto-login failed or no credentials set.")
            self.log("Set FB_EMAIL and FB_PASSWORD env vars for auto-login.")
            return False
        except Exception as e:
            logging.error(f"[Facebook] Login error: {e}")
            return False

    def _auto_login(self):
        """Attempt to log in using FB_EMAIL / FB_PASSWORD env vars."""
        from selenium.webdriver.common.by import By

        email = os.environ.get("FB_EMAIL", "")
        password = os.environ.get("FB_PASSWORD", "")
        if not email or not password:
            return False

        self.log("Attempting auto-login...")
        try:
            self.driver.get("https://www.facebook.com/login")
            self.inject_stealth()
            self.human_delay(2, 4)

            email_field = self.driver.find_element(By.NAME, "email")
            pass_field = self.driver.find_element(By.NAME, "pass")

            # Type like a human — character by character with small delays
            email_field.clear()
            for char in email:
                email_field.send_keys(char)
                time.sleep(random.uniform(0.03, 0.12))

            self.human_delay(0.3, 0.8)

            pass_field.clear()
            for char in password:
                pass_field.send_keys(char)
                time.sleep(random.uniform(0.03, 0.12))

            self.human_delay(0.5, 1.5)

            # Submit by pressing Enter on the password field (most reliable)
            from selenium.webdriver.common.keys import Keys
            pass_field.send_keys(Keys.RETURN)

            # Wait for redirect after login
            self.human_delay(5, 8)

            if self._is_logged_in():
                self.log("Auto-login successful.")
                return True

            # FB might show a checkpoint/2FA page — wait a bit longer
            self.human_delay(3, 5)
            if self._is_logged_in():
                self.log("Auto-login successful (after checkpoint).")
                return True

            self.log("Auto-login failed — may need 2FA or account review.")
            self.capture_screenshot("fb_login_failed")
            return False

        except Exception as e:
            self.log(f"Auto-login error: {e}")
            self.capture_screenshot("fb_login_error")
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
