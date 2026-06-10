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

# Structural patterns for parsing listing cards. FB's generated CSS classes
# (x3ct3a4, x1gslohp, …) rotate between builds and differ between the
# logged-in and anon views — they broke silently in prod. The /marketplace/
# item/<id>/ href format and the span-text shape (price, title, "City, ST",
# "149K miles") have been stable for years, so parse those instead.
ITEM_ID_RE = re.compile(r"/marketplace/item/(\d+)")
# "$14,000" or "MX$10,200" — a span holding exactly one price
PRICE_RE = re.compile(r"^[A-Z]{0,3}\$[\d,.]+$")
# "Salt Lake City, UT"
LOCATION_RE = re.compile(r"^[^$]+,\s*[A-Z]{2}$")
# "149K miles", "12K miles · Dealership", "1,490 km"
MILEAGE_RE = re.compile(r"^[\d,.]+\s*K?\s*(?:miles|mi|km)\b", re.I)


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
            listings = soup.select("a[href*='/marketplace/item/']")

            found_before = self._listing_count
            seen_ids = set()
            for item in listings:
                self._process_listing(item, car_query, seen_ids)
            self.log(f"  {car_query}: {len(listings)} cards → "
                     f"{self._listing_count - found_before} inserted")

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

    def _process_listing(self, item, car_query, seen_ids=None):
        """Parse one search-result card from its /marketplace/item/ anchor.

        Each card anchor contains the listing fields as span text (each
        rendered several times in nested visual/accessibility copies):
        price, title, "City, ST", "NNNK miles".  Discounted listings
        prepend a combined "$new$old" span; dealer listings append
        " · Dealership" to the mileage span.
        """
        href = item.get("href") or ""
        id_match = ITEM_ID_RE.search(href)
        if not id_match:
            return
        item_id = id_match.group(1)
        if seen_ids is not None:
            if item_id in seen_ids:
                return
            seen_ids.add(item_id)
        # Canonical URL — strips ?ref=search&… tracking params so the
        # (href, source) upsert key stays stable across scrapes
        full_href = f"https://www.facebook.com/marketplace/item/{item_id}/"

        # Dedup span texts preserving order
        texts = []
        for span in item.find_all("span"):
            t = span.get_text(strip=True)
            if t and t not in texts:
                texts.append(t)

        price_str, city, miles = "", "", "N/A"
        rest = []
        for t in texts:
            if not price_str and PRICE_RE.match(t):
                price_str = t   # first single-price span = current price
            elif miles == "N/A" and MILEAGE_RE.match(t):
                miles = t
            elif not city and LOCATION_RE.match(t):
                city = t
            elif "$" not in t:
                rest.append(t)
        if not price_str or not rest:
            return
        title = max(rest, key=len)

        price_parts = price_str.split("$")[1:]
        if not price_parts:
            return
        price = price_parts[0]

        img_tag = item.find("img", {"src": True})
        image_url = img_tag["src"] if img_tag else ""

        # Title type — Facebook occasionally includes it in listing text
        title_type = ""
        full_text = f"{title} {city} {miles}".lower()
        if "salvage" in full_text:
            title_type = "salvage"
        elif "rebuilt" in full_text:
            title_type = "rebuilt"
        elif "clean title" in full_text:
            title_type = "clean"

        # Dealer listings tag the mileage span: "145K miles · Dealership"
        seller_type = ""
        if "dealership" in miles.lower():
            seller_type = "dealer"
            miles = miles.split("·")[0].strip()

        self.counted_insert(
            car_query=car_query, href=full_href, image_url=image_url,
            price=price, car_name=title, location=city,
            mileage_raw=miles, source=self.SOURCE_NAME,
            title_type=title_type, seller_type=seller_type,
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
        """Establish an authenticated FB session, in priority order:

          1. Native profile session — with a persistent profile
             (FB_PROFILE_DIR) the device's own datr+xs cookies live on
             disk and survive between runs, so most runs are already
             logged in with zero work. This is the durable path.
          2. Account-picker "Continue" — device token still valid, one
             click resumes the session without a password.
          3. Credential auto-login — FB_EMAIL/FB_PASSWORD (+ FB_TOTP_SECRET
             for 2FA), seeded by the backup pickle. Heals a lapsed session
             headlessly.

        We must *navigate and validate*, not trust on-disk state: stale
        cookies load fine but FB silently serves the anon-view marketplace
        (~1/3 of listings, prices/titles stripped). Crucially we check the
        native session BEFORE injecting the backup pickle — re-injecting
        stale pickle cookies over a live persistent-profile session would
        knock it back to the anon view.
        """
        self.log("Checking login status...")
        try:
            self.driver.get("https://www.facebook.com/")
            self.inject_stealth()
            time.sleep(2)

            # 1. Persistent profile may already hold a live session
            if self._is_logged_in():
                self.log("Logged in (native profile session).")
                self._save_cookies()  # refresh the backup pickle
                return True

            # 2. Account-picker Continue (valid device token, no password)
            if self._click_profile_continue() and self._is_logged_in():
                self.log("Logged in via account-picker Continue.")
                self._save_cookies()
                return True

            # 3. Re-seed from the backup pickle, then validate again — this
            #    bootstraps a brand-new persistent profile from a synced
            #    session and is a no-op if the profile is already current.
            if self._load_cookies():
                self.driver.get("https://www.facebook.com/")
                self.inject_stealth()
                time.sleep(2)
                if self._is_logged_in():
                    self.log("Logged in (restored from backup cookies).")
                    self._save_cookies()
                    return True
                if self._click_profile_continue() and self._is_logged_in():
                    self.log("Logged in via Continue (after cookie restore).")
                    self._save_cookies()
                    return True

            # Stale c_user pushes FB into an account-picker / recovery flow
            # (no email field), so auto_login can't find its inputs. Clear
            # cookies first so FB serves the canonical email+password form.
            try:
                self.driver.delete_all_cookies()
            except Exception:
                pass

            # 4. Credential auto-login (FB_EMAIL/FB_PASSWORD/FB_TOTP_SECRET)
            if self._auto_login():
                self._save_cookies()
                return True

            logging.warning(
                "[Facebook] SESSION EXPIRED — no live profile session, no "
                "valid backup cookies, and FB_EMAIL/FB_PASSWORD auto-login "
                "unavailable or failed. Skipping FB scrape (anon-view yields "
                "~1/3 of real listings). Set FB_PROFILE_DIR + credentials, "
                "or re-seed fb_cookies.pkl from a logged-in browser.")
            return False
        except Exception as e:
            logging.error(f"[Facebook] Login error: {e}")
            return False

    def _click_profile_continue(self):
        """Try to resume a session from FB's account-picker page.

        FB sometimes shows a picker with the profile name, a "Continue"
        button, and "Use another profile". If the profile's device token
        is still valid, Continue resumes the session with one click; if
        not, FB asks for the password and the subsequent _is_logged_in()
        check fails, falling through to credential auto-login. Returns
        True if the button was found and clicked.
        """
        try:
            if "Use another profile" not in self.driver.page_source:
                return False
            clicked = self.driver.execute_script("""
                const els = document.querySelectorAll(
                    "div[role='button'], button");
                for (const el of els) {
                    if ((el.textContent || '').trim() === 'Continue') {
                        el.click();
                        return true;
                    }
                }
                return false;
            """)
            if clicked:
                self.log("Account picker detected — clicked Continue.")
                self.human_delay(4, 7)
            return bool(clicked)
        except Exception:
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

            # The /login route normally serves the full email+password form,
            # but a recognized-device session that just lapsed shows a
            # password-ONLY modal (no email field) — handle both. The
            # password field has been name="pass" for years.
            try:
                pass_field = self.driver.find_element(By.NAME, "pass")
            except Exception:
                pass_field = self.driver.find_element(
                    By.CSS_SELECTOR, "input[type='password']")

            email_field = None
            try:
                email_field = self.driver.find_element(By.NAME, "email")
            except Exception:
                self.log("No email field — password-only (recognized-device) "
                         "modal; filling password only.")

            # Type like a human — character by character with small delays
            if email_field is not None:
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

            # FB often lands on /two_step_verification/authentication/ before
            # the home page. Handle the TOTP code automatically if a secret
            # is configured.
            if not self._handle_2fa_if_present():
                self.capture_screenshot("fb_2fa_failed")
                return False

            if self._is_logged_in():
                self.log("Auto-login successful.")
                return True

            # FB might still be on a checkpoint page — wait a bit longer
            self.human_delay(3, 5)
            if self._is_logged_in():
                self.log("Auto-login successful (after checkpoint).")
                return True

            self.log("Auto-login failed — landed on an unrecognised page.")
            self.capture_screenshot("fb_login_failed")
            return False

        except Exception as e:
            self.log(f"Auto-login error: {e}")
            self.capture_screenshot("fb_login_error")
            return False

    def _handle_2fa_if_present(self):
        """If FB is on the TOTP page, compute the code from FB_TOTP_SECRET
        and submit it. Returns True if we either weren't on a 2FA page or
        successfully submitted the code; False if 2FA was required but we
        couldn't complete it (missing secret, missing input, etc.).
        """
        url = self.driver.current_url
        if "two_step_verification" not in url and "checkpoint" not in url:
            return True   # no 2FA prompt — nothing to do

        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys

        secret_raw = os.environ.get("FB_TOTP_SECRET", "")
        # Authenticator-style secrets are often shown with spaces — strip them.
        secret = secret_raw.replace(" ", "").strip()
        if not secret:
            self.log("2FA prompt detected but FB_TOTP_SECRET is not set.")
            return False

        try:
            import pyotp
        except ImportError:
            self.log("2FA prompt detected but pyotp is not installed.")
            return False

        code = pyotp.TOTP(secret).now()
        self.log(f"2FA prompt detected — submitting TOTP code "
                 f"(len={len(code)}, never logged in plain).")

        # FB's 2FA code input has been named `approvals_code` for years.
        # Fall back to any text/number input if that selector ever changes.
        code_field = None
        for finder in (
            lambda: self.driver.find_element(By.NAME, "approvals_code"),
            lambda: self.driver.find_element(
                By.CSS_SELECTOR, "input[type='text'], input[type='number']"),
        ):
            try:
                code_field = finder()
                break
            except Exception:
                continue
        if code_field is None:
            self.log("Couldn't locate the 2FA code input field.")
            return False

        try:
            code_field.clear()
        except Exception:
            pass
        for ch in code:
            code_field.send_keys(ch)
            time.sleep(random.uniform(0.05, 0.15))
        self.human_delay(0.4, 1.0)
        code_field.send_keys(Keys.RETURN)

        # FB may show a "Trust this browser?" interstitial after the code.
        # Wait, then it usually lands on the home feed (or another checkpoint).
        self.human_delay(6, 10)
        return True

    def _is_logged_in(self):
        """True only if FB is serving the *authenticated* view.

        Primary signal is the `c_user` cookie — Facebook sets it only for
        an authenticated session, so it cleanly distinguishes the real
        logged-in view from the anon marketplace, the account-picker, a
        2FA page, or a reCAPTCHA challenge (none of which carry c_user).
        A pure HTML denylist used to false-positive on those interstitials
        (they lack the old negative markers), so the cookie comes first.
        """
        try:
            url = self.driver.current_url
            # Login / challenge / checkpoint URLs are never the authed view
            if any(x in url for x in ("/login", "/checkpoint",
                                      "two_step_verification", "/recover")):
                return False

            # Positive signal: the authenticated-user cookie
            try:
                if self.driver.get_cookie("c_user"):
                    return True
            except Exception:
                pass

            # Fallback HTML negatives (cookie unreadable for some reason)
            page = self.driver.page_source.lower()
            if 'id="loginbutton"' in page or 'name="email"' in page:
                return False
            if 'href="/login' in page or "create new account" in page:
                return False
            return False   # no c_user and no positive proof → not logged in
        except Exception:
            return False

    def _save_cookies(self):
        """Persist the current session cookies to the backup pickle.

        Facebook hands out c_user/xs as SESSION cookies (no expiry) on
        automated logins, so get_cookies() returns them without an expiry
        and Firefox would drop them on quit. Force a far-future expiry so
        the backup is genuinely persistent and _load_cookies() can restore
        a live session on the next run WITHOUT going through /login (which
        triggers reCAPTCHA headlessly).
        """
        cookies = self.driver.get_cookies()
        far_future = int(time.time()) + 60 * 60 * 24 * 60   # ~60 days
        for c in cookies:
            if not c.get("expiry"):
                c["expiry"] = far_future
        with open(COOKIE_FILE, "wb") as f:
            pickle.dump(cookies, f)

    def _load_cookies(self):
        if not COOKIE_FILE.exists():
            return False
        try:
            with open(COOKIE_FILE, "rb") as f:
                cookies = pickle.load(f)
            self.driver.get("https://www.facebook.com/")
            self.inject_stealth()
            for cookie in cookies:
                # sameSite values from get_cookies (None/"No Restriction")
                # are not valid add_cookie inputs — drop the key.
                cookie.pop("sameSite", None)
                # Preserve expiry (coerced to int) so restored auth cookies
                # stay PERSISTENT. Stripping it made them session cookies,
                # which Firefox drops on quit — defeating the restore.
                if "expiry" in cookie:
                    try:
                        cookie["expiry"] = int(cookie["expiry"])
                    except (TypeError, ValueError):
                        cookie.pop("expiry", None)
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
