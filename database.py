"""Database connection, schema, migration, and CRUD operations."""

import logging
import os
import sqlite3
from pathlib import Path

from parsing import parse_price, parse_mileage, extract_year

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("DATA_DIR", SCRIPT_DIR))
DB_PATH = DATA_DIR / "marketplace_listings.db"


class Database:
    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        self.conn = None
        self.cur = None

    def open(self):
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
        self._migrate()
        self._create_tables()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cur = None

    def _create_tables(self):
        self.cur.executescript("""
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                href TEXT,
                image_url TEXT,
                price REAL,
                car_name TEXT,
                car_query TEXT,
                location TEXT,
                mileage REAL,
                year INTEGER,
                source TEXT DEFAULT 'facebook',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                deleted_at TEXT,
                trim TEXT,
                seller TEXT,
                condition TEXT,
                deal_rating TEXT,
                accident_history TEXT,
                distance TEXT,
                title_type TEXT,
                description TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_listings_href_source ON listings(href, source);
            CREATE INDEX IF NOT EXISTS idx_listings_car_query ON listings(car_query);
            CREATE INDEX IF NOT EXISTS idx_listings_href ON listings(href);
            CREATE INDEX IF NOT EXISTS idx_listings_source ON listings(source);

            CREATE TABLE IF NOT EXISTS average_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                car_query TEXT,
                year INTEGER,
                title_group TEXT DEFAULT 'all',
                avg_lower_mileage_price REAL,
                avg_higher_mileage_price REAL,
                UNIQUE(car_query, year, title_group)
            );

            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_href TEXT NOT NULL,
                source TEXT NOT NULL,
                old_price REAL NOT NULL,
                new_price REAL NOT NULL,
                changed_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_price_history_href
                ON price_history(listing_href, source);

            CREATE TABLE IF NOT EXISTS vin_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vin TEXT NOT NULL UNIQUE,
                make TEXT,
                model TEXT,
                year INTEGER,
                trim TEXT,
                body_class TEXT,
                drive_type TEXT,
                fuel_type TEXT,
                engine TEXT,
                displacement TEXT,
                cylinders TEXT,
                plant_city TEXT,
                plant_country TEXT,
                error_code TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_vin_cache_vin ON vin_cache(vin);

            CREATE TABLE IF NOT EXISTS vehicle_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                make TEXT NOT NULL,
                model TEXT NOT NULL,
                year INTEGER NOT NULL,
                overall_rating INTEGER,
                front_crash_rating INTEGER,
                side_crash_rating INTEGER,
                rollover_rating INTEGER,
                complaints_count INTEGER DEFAULT 0,
                recalls_count INTEGER DEFAULT 0,
                mpg_city INTEGER,
                mpg_highway INTEGER,
                mpg_combined INTEGER,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(make, model, year)
            );
            CREATE INDEX IF NOT EXISTS idx_vehicle_ratings_lookup
                ON vehicle_ratings(make, model, year);

            CREATE TABLE IF NOT EXISTS vehicle_recalls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                make TEXT NOT NULL,
                model TEXT NOT NULL,
                year INTEGER NOT NULL,
                campaign_number TEXT NOT NULL,
                component TEXT,
                summary TEXT,
                consequence TEXT,
                remedy TEXT,
                report_date TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(make, model, year, campaign_number)
            );
            CREATE INDEX IF NOT EXISTS idx_vehicle_recalls_lookup
                ON vehicle_recalls(make, model, year);

            CREATE TABLE IF NOT EXISTS scrape_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT DEFAULT 'running',
                listings_found INTEGER DEFAULT 0,
                listings_new INTEGER DEFAULT 0,
                listings_updated INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                error_message TEXT,
                screenshot_path TEXT,
                duration_seconds REAL
            );
            CREATE INDEX IF NOT EXISTS idx_scrape_runs_source
                ON scrape_runs(source, started_at);
        """)
        self.conn.commit()

    def _migrate(self):
        """Add source column to existing DBs and backfill Facebook listings."""
        self.cur.execute("PRAGMA table_info(listings)")
        rows = self.cur.fetchall()
        if not rows:
            return  # Table doesn't exist yet; _create_tables will handle it
        columns = [row["name"] if isinstance(row, sqlite3.Row) else row[1] for row in rows]
        if "source" not in columns:
            logging.info("Migrating DB: adding 'source' column...")
            self.cur.execute("ALTER TABLE listings ADD COLUMN source TEXT DEFAULT 'facebook'")
            self.cur.execute(
                "UPDATE listings SET href = 'https://www.facebook.com' || href "
                "WHERE source = 'facebook' AND href NOT LIKE 'http%'"
            )
            self.cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_listings_href_source "
                "ON listings(href, source)"
            )
            self.conn.commit()
            logging.info("Migration complete.")

        if "updated_at" not in columns:
            logging.info("Migrating DB: adding timestamp columns...")
            self.cur.execute("ALTER TABLE listings ADD COLUMN updated_at TEXT")
            self.cur.execute("ALTER TABLE listings ADD COLUMN deleted_at TEXT")
            self.cur.execute("UPDATE listings SET updated_at = COALESCE(created_at, datetime('now'))")
            self.conn.commit()
            logging.info("Timestamp migration complete.")

        if "trim" not in columns:
            logging.info("Migrating DB: adding detail columns...")
            for col in ["trim", "seller", "condition", "deal_rating", "accident_history", "distance"]:
                try:
                    self.cur.execute(f"ALTER TABLE listings ADD COLUMN {col} TEXT")
                except sqlite3.OperationalError:
                    pass  # Column already exists
            self.conn.commit()
            logging.info("Detail columns migration complete.")

        if "title_type" not in columns:
            logging.info("Migrating DB: adding title_type column...")
            try:
                self.cur.execute("ALTER TABLE listings ADD COLUMN title_type TEXT")
                self.conn.commit()
                logging.info("title_type migration complete.")
            except sqlite3.OperationalError:
                pass

        if "description" not in columns:
            logging.info("Migrating DB: adding description column...")
            try:
                self.cur.execute("ALTER TABLE listings ADD COLUMN description TEXT")
                self.conn.commit()
                logging.info("description migration complete.")
            except sqlite3.OperationalError:
                pass

        if "vin" not in columns:
            logging.info("Migrating DB: adding vin column...")
            try:
                self.cur.execute("ALTER TABLE listings ADD COLUMN vin TEXT")
                self.conn.commit()
                logging.info("vin migration complete.")
            except sqlite3.OperationalError:
                pass

        if "enriched_at" not in columns:
            logging.info("Migrating DB: adding enriched_at column...")
            try:
                self.cur.execute("ALTER TABLE listings ADD COLUMN enriched_at TEXT")
                # Backfill: listings that already have title_type were already enriched
                self.cur.execute(
                    "UPDATE listings SET enriched_at = updated_at "
                    "WHERE title_type IS NOT NULL AND title_type != ''"
                )
                self.conn.commit()
                logging.info("enriched_at migration complete.")
            except sqlite3.OperationalError:
                pass

        if "owner_count" not in columns:
            logging.info("Migrating DB: adding owner_count and carfax_url columns...")
            for col in ["owner_count", "carfax_url"]:
                try:
                    self.cur.execute(f"ALTER TABLE listings ADD COLUMN {col} TEXT")
                except sqlite3.OperationalError:
                    pass
            self.conn.commit()
            logging.info("owner_count/carfax_url migration complete.")

        if "listed_at" not in columns:
            logging.info("Migrating DB: adding listed_at column...")
            try:
                self.cur.execute("ALTER TABLE listings ADD COLUMN listed_at TEXT")
                self.conn.commit()
                logging.info("listed_at migration complete.")
            except sqlite3.OperationalError:
                pass

        if "image_urls" not in columns:
            logging.info("Migrating DB: adding image_urls column...")
            try:
                self.cur.execute("ALTER TABLE listings ADD COLUMN image_urls TEXT")
                self.conn.commit()
                logging.info("image_urls migration complete.")
            except sqlite3.OperationalError:
                pass

        # Migrate vehicle_ratings to include MPG columns
        self.cur.execute("PRAGMA table_info(vehicle_ratings)")
        vr_rows = self.cur.fetchall()
        if vr_rows:
            vr_cols = [r["name"] if isinstance(r, sqlite3.Row) else r[1] for r in vr_rows]
            for col in ["mpg_city", "mpg_highway", "mpg_combined"]:
                if col not in vr_cols:
                    try:
                        self.cur.execute(f"ALTER TABLE vehicle_ratings ADD COLUMN {col} INTEGER")
                    except sqlite3.OperationalError:
                        pass
            self.conn.commit()

        # Migrate average_prices to include title_group
        self.cur.execute("PRAGMA table_info(average_prices)")
        avg_rows = self.cur.fetchall()
        if avg_rows:
            avg_cols = [r["name"] if isinstance(r, sqlite3.Row) else r[1] for r in avg_rows]
            if "title_group" not in avg_cols:
                logging.info("Migrating DB: rebuilding average_prices with title_group...")
                self.cur.execute("DROP TABLE IF EXISTS average_prices")
                self.conn.commit()
                logging.info("average_prices dropped (will be recreated with title_group).")

    # ── Inserts ────────────────────────────────────────────────────

    @staticmethod
    def _normalize_href(href):
        """Strip tracking query params from Facebook Marketplace URLs.

        FB appends session-specific tracking UUIDs (browse_serp, referral_code,
        __tn__, etc.) that change every scrape, making the same listing appear
        as a different URL each time.  The actual identity is the path, e.g.
        /marketplace/item/12345/.
        """
        if not href:
            return href
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(href)
        # Keep scheme, netloc, path — drop params, query, fragment
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path,
                           '', '', ''))

    def insert_listing(self, *, car_query, href, image_url, price, car_name,
                       location, mileage_raw, source, deleted_set=None,
                       trim="", seller="", condition="", deal_rating="",
                       accident_history="", distance="", title_type="",
                       owner_count="", carfax_url=""):
        """Insert or update a listing, parsing raw price/mileage/year."""
        href = self._normalize_href(href)

        if deleted_set and href in deleted_set:
            return

        price_val = parse_price(price)
        mileage_val = parse_mileage(mileage_raw)
        year_val = extract_year(car_name)

        # Track price changes before upsert
        if price_val is not None:
            try:
                self.cur.execute(
                    "SELECT price FROM listings WHERE href = ? AND source = ?",
                    (href, source))
                existing = self.cur.fetchone()
                if existing and existing["price"] is not None:
                    old_price = existing["price"]
                    if old_price != price_val:
                        self.cur.execute(
                            "INSERT INTO price_history "
                            "(listing_href, source, old_price, new_price) "
                            "VALUES (?, ?, ?, ?)",
                            (href, source, old_price, price_val))
            except sqlite3.Error:
                pass  # Don't let history tracking break inserts

        try:
            self.cur.execute("""
                INSERT INTO listings
                    (href, image_url, price, car_name, car_query, location,
                     mileage, year, source, updated_at,
                     trim, seller, condition, deal_rating, accident_history,
                     distance, title_type, owner_count, carfax_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(href, source) DO UPDATE SET
                    price = excluded.price,
                    image_url = COALESCE(excluded.image_url, image_url),
                    updated_at = CURRENT_TIMESTAMP,
                    trim = COALESCE(excluded.trim, trim),
                    seller = COALESCE(excluded.seller, seller),
                    deal_rating = COALESCE(excluded.deal_rating, deal_rating),
                    accident_history = COALESCE(excluded.accident_history, accident_history),
                    distance = COALESCE(excluded.distance, distance),
                    title_type = COALESCE(excluded.title_type, title_type),
                    owner_count = COALESCE(excluded.owner_count, owner_count),
                    carfax_url = COALESCE(excluded.carfax_url, carfax_url)
            """, (href, image_url, price_val, car_name, car_query, location,
                  mileage_val, year_val, source,
                  trim or None, seller or None, condition or None,
                  deal_rating or None, accident_history or None, distance or None,
                  title_type or None, owner_count or None, carfax_url or None))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB insert error: {e}")

    # ── Queries ────────────────────────────────────────────────────

    def get_listings_for_query(self, car_query):
        self.cur.execute(
            "SELECT id, car_name FROM listings WHERE car_query = ?", (car_query,)
        )
        return self.cur.fetchall()

    def get_priced_listings(self, car_query):
        self.cur.execute(
            "SELECT price, mileage, year, title_type FROM listings "
            "WHERE car_query = ? AND price IS NOT NULL AND year IS NOT NULL "
            "AND deleted_at IS NULL",
            (car_query,)
        )
        return self.cur.fetchall()

    def get_deal_candidates(self, car_query):
        self.cur.execute(
            "SELECT href, price, mileage, year, location, source, "
            "image_url, car_name, created_at, updated_at, "
            "trim, seller, condition, deal_rating, accident_history, distance, "
            "title_type, vin, description, owner_count, carfax_url, listed_at, "
            "image_urls "
            "FROM listings "
            "WHERE car_query = ? AND price IS NOT NULL AND deleted_at IS NULL",
            (car_query,)
        )
        return self.cur.fetchall()

    def get_averages(self, car_query):
        """Get all averages for a car_query, keyed by (year, title_group)."""
        self.cur.execute(
            "SELECT year, title_group, avg_lower_mileage_price, "
            "avg_higher_mileage_price "
            "FROM average_prices WHERE car_query = ?",
            (car_query,)
        )
        return {(row[0], row[1]): (row[2], row[3]) for row in self.cur.fetchall()}

    def get_all_deals(self):
        """Get all listings flagged as deals (used by web UI)."""
        self.cur.execute(
            "SELECT href, image_url, price, car_name, car_query, location, "
            "mileage, year, source FROM listings WHERE price IS NOT NULL "
            "ORDER BY price ASC"
        )
        return self.cur.fetchall()

    def backfill_listed_at(self):
        """Parse 'Listed N days/weeks ago' from FB descriptions into listed_at."""
        from parsing import parse_listed_date
        self.cur.execute(
            "SELECT id, description, updated_at FROM listings "
            "WHERE source = 'facebook' AND listed_at IS NULL "
            "AND description IS NOT NULL AND description != ''"
        )
        rows = self.cur.fetchall()
        updated = 0
        for row in rows:
            desc_flat = row["description"].replace("\n", " ")
            scrape_date = None
            if row["updated_at"]:
                try:
                    from datetime import datetime
                    scrape_date = datetime.fromisoformat(row["updated_at"])
                except (ValueError, TypeError):
                    pass
            listed_at = parse_listed_date(desc_flat, scrape_date)
            if listed_at:
                self.cur.execute(
                    "UPDATE listings SET listed_at = ? WHERE id = ?",
                    (listed_at, row["id"]))
                updated += 1
        if updated:
            self.conn.commit()
            logging.info(f"Backfilled listed_at for {updated} Facebook listings")

    def delete_listing(self, href):
        self.cur.execute(
            "UPDATE listings SET deleted_at = CURRENT_TIMESTAMP WHERE href = ?",
            (href,))
        self.conn.commit()

    def delete_by_id(self, row_id):
        self.cur.execute(
            "UPDATE listings SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?",
            (row_id,))
        self.conn.commit()

    def mark_stale(self, source, days_old=30):
        """Soft-delete listings from a source that haven't been updated recently."""
        self.cur.execute(
            "UPDATE listings SET deleted_at = CURRENT_TIMESTAMP "
            "WHERE source = ? AND deleted_at IS NULL "
            "AND updated_at < datetime('now', ?)",
            (source, f'-{days_old} days'))
        count = self.cur.rowcount
        self.conn.commit()
        return count

    def get_analytics_data(self):
        """Return all active listings with key fields for analytics."""
        self.cur.execute(
            "SELECT car_query, price, mileage, year, source, location, "
            "seller, deal_rating, distance, created_at, "
            "title_type, trim, accident_history, condition, vin "
            "FROM listings WHERE deleted_at IS NULL AND price IS NOT NULL"
        )
        return self.cur.fetchall()

    def get_price_drops_summary(self, days=30):
        """Get price drop events from the last N days."""
        self.cur.execute(
            "SELECT ph.listing_href, ph.old_price, ph.new_price, "
            "ph.changed_at, l.car_query, l.car_name "
            "FROM price_history ph "
            "LEFT JOIN listings l ON l.href = ph.listing_href "
            "WHERE ph.new_price < ph.old_price "
            "AND ph.changed_at >= datetime('now', ?) "
            "ORDER BY ph.changed_at DESC",
            (f'-{days} days',))
        return self.cur.fetchall()

    def get_analytics_averages(self):
        """Return 'all' averages for analytics charts."""
        self.cur.execute(
            "SELECT car_query, year, avg_lower_mileage_price, avg_higher_mileage_price "
            "FROM average_prices WHERE title_group = 'all'"
        )
        return self.cur.fetchall()

    # ── Price History ─────────────────────────────────────────────

    def get_price_history(self, href, source=None):
        """Get full price history for a listing, newest first."""
        if source:
            self.cur.execute(
                "SELECT old_price, new_price, changed_at FROM price_history "
                "WHERE listing_href = ? AND source = ? ORDER BY changed_at DESC",
                (href, source))
        else:
            self.cur.execute(
                "SELECT old_price, new_price, changed_at FROM price_history "
                "WHERE listing_href = ? ORDER BY changed_at DESC",
                (href,))
        return self.cur.fetchall()

    def get_price_history_batch(self, hrefs):
        """Get the latest price change for multiple listings at once."""
        if not hrefs:
            return {}
        placeholders = ",".join("?" * len(hrefs))
        self.cur.execute(f"""
            SELECT listing_href, old_price, new_price, changed_at
            FROM price_history
            WHERE listing_href IN ({placeholders})
            ORDER BY changed_at DESC
        """, hrefs)
        result = {}
        for row in self.cur.fetchall():
            href = row["listing_href"]
            if href not in result:  # keep only newest change per listing
                result[href] = {
                    "old_price": row["old_price"],
                    "new_price": row["new_price"],
                    "changed_at": row["changed_at"],
                }
        return result

    # ── Listings by href (for favorites) ──────────────────────────

    def get_listings_by_hrefs(self, hrefs):
        """Fetch full listing data for a set of hrefs."""
        if not hrefs:
            return []
        placeholders = ",".join("?" * len(hrefs))
        self.cur.execute(f"""
            SELECT href, image_url, price, car_name, car_query, location,
                   mileage, year, source, created_at, updated_at,
                   trim, seller, condition, deal_rating, accident_history,
                   distance, title_type, vin, image_urls
            FROM listings
            WHERE href IN ({placeholders}) AND deleted_at IS NULL
            ORDER BY updated_at DESC
        """, list(hrefs))
        return self.cur.fetchall()

    # ── Averages ───────────────────────────────────────────────────

    def upsert_average(self, car_query, year, avg_lower, avg_higher,
                       title_group="all"):
        self.cur.execute("""
            INSERT OR REPLACE INTO average_prices
                (car_query, year, title_group,
                 avg_lower_mileage_price, avg_higher_mileage_price)
            VALUES (?, ?, ?, ?, ?)
        """, (car_query, year, title_group, avg_lower, avg_higher))
        self.conn.commit()

    # ── Vehicle Ratings (NHTSA cache) ─────────────────────────────

    def get_vehicle_rating(self, make, model, year):
        """Get cached NHTSA rating for a vehicle."""
        self.cur.execute(
            "SELECT overall_rating, front_crash_rating, side_crash_rating, "
            "rollover_rating, complaints_count, recalls_count, "
            "mpg_city, mpg_highway, mpg_combined, fetched_at "
            "FROM vehicle_ratings "
            "WHERE make = ? AND model = ? AND year = ?",
            (make.lower(), model.lower(), year))
        row = self.cur.fetchone()
        if not row:
            return None
        return {
            "overall_rating": row["overall_rating"],
            "front_crash_rating": row["front_crash_rating"],
            "side_crash_rating": row["side_crash_rating"],
            "rollover_rating": row["rollover_rating"],
            "complaints_count": row["complaints_count"] or 0,
            "recalls_count": row["recalls_count"] or 0,
            "mpg_city": row["mpg_city"],
            "mpg_highway": row["mpg_highway"],
            "mpg_combined": row["mpg_combined"],
            "fetched_at": row["fetched_at"],
        }

    def upsert_vehicle_rating(self, *, make, model, year,
                               overall_rating, front_crash, side_crash,
                               rollover, complaints, recalls,
                               mpg_city=None, mpg_highway=None,
                               mpg_combined=None):
        """Insert or update a cached NHTSA rating (+ optional MPG data)."""
        try:
            self.cur.execute("""
                INSERT INTO vehicle_ratings
                    (make, model, year, overall_rating, front_crash_rating,
                     side_crash_rating, rollover_rating,
                     complaints_count, recalls_count,
                     mpg_city, mpg_highway, mpg_combined, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        CURRENT_TIMESTAMP)
                ON CONFLICT(make, model, year) DO UPDATE SET
                    overall_rating = excluded.overall_rating,
                    front_crash_rating = excluded.front_crash_rating,
                    side_crash_rating = excluded.side_crash_rating,
                    rollover_rating = excluded.rollover_rating,
                    complaints_count = excluded.complaints_count,
                    recalls_count = excluded.recalls_count,
                    mpg_city = COALESCE(excluded.mpg_city, mpg_city),
                    mpg_highway = COALESCE(excluded.mpg_highway, mpg_highway),
                    mpg_combined = COALESCE(excluded.mpg_combined, mpg_combined),
                    fetched_at = CURRENT_TIMESTAMP
            """, (make.lower(), model.lower(), year,
                  overall_rating, front_crash, side_crash,
                  rollover, complaints, recalls,
                  mpg_city, mpg_highway, mpg_combined))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB vehicle rating upsert error: {e}")

    def mark_enriched(self, href):
        """Mark a listing as enriched (attempted) without changing any data."""
        try:
            self.cur.execute(
                "UPDATE listings SET enriched_at = CURRENT_TIMESTAMP WHERE href = ?",
                (href,))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB mark_enriched error: {e}")

    def update_title_type(self, href, title_type):
        """Update the title_type for a specific listing."""
        try:
            self.cur.execute(
                "UPDATE listings SET title_type = ?, updated_at = CURRENT_TIMESTAMP, "
                "enriched_at = CURRENT_TIMESTAMP "
                "WHERE href = ?", (title_type, href))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB update title_type error: {e}")

    def update_listing_details(self, href, **kwargs):
        """Update multiple detail fields for a listing."""
        allowed = {"title_type", "trim", "seller", "condition",
                   "deal_rating", "accident_history", "description", "vin",
                   "image_urls"}
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k in allowed and v:
                sets.append(f"{k} = ?")
                vals.append(v)
        if not sets:
            return
        sets.append("updated_at = CURRENT_TIMESTAMP")
        sets.append("enriched_at = CURRENT_TIMESTAMP")
        vals.append(href)
        try:
            self.cur.execute(
                f"UPDATE listings SET {', '.join(sets)} WHERE href = ?", vals)
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB update details error: {e}")

    def get_listings_missing_title_type(self, source=None, limit=50):
        """Get listings that need enrichment.

        Only returns listings that have NEVER been enriched (enriched_at IS NULL).
        Prioritizes: missing title_type first, then missing description.
        """
        q = ("SELECT id, href, car_name FROM listings "
             "WHERE enriched_at IS NULL "
             "AND deleted_at IS NULL")
        params = []
        if source:
            q += " AND source = ?"
            params.append(source)
        # Sort: missing title_type first (most important), then by newest
        q += (" ORDER BY "
              "CASE WHEN title_type IS NULL OR title_type = '' THEN 0 ELSE 1 END, "
              "created_at DESC "
              "LIMIT ?")
        params.append(limit)
        self.cur.execute(q, params)
        return self.cur.fetchall()

    def backfill_title_types(self):
        """Scan car_name text for title keywords and backfill title_type."""
        updated = 0
        rows = self.cur.execute(
            "SELECT id, car_name FROM listings "
            "WHERE (title_type IS NULL OR title_type = '') "
            "AND deleted_at IS NULL"
        ).fetchall()
        for row in rows:
            text = (row["car_name"] or "").lower()
            title_type = None
            if "salvage" in text:
                title_type = "salvage"
            elif "rebuilt" in text:
                title_type = "rebuilt"
            elif "lemon" in text:
                title_type = "lemon"
            elif "clean title" in text:
                title_type = "clean"
            if title_type:
                self.cur.execute(
                    "UPDATE listings SET title_type = ? WHERE id = ?",
                    (title_type, row["id"]))
                updated += 1
        self.conn.commit()
        if updated:
            logging.info(f"Backfilled title_type for {updated} listings from car_name text.")
        return updated

    # ── VIN Cache ──────────────────────────────────────────────────

    def get_vin_data(self, vin):
        """Get cached VIN decode data."""
        self.cur.execute(
            "SELECT vin, make, model, year, trim, body_class, drive_type, "
            "fuel_type, engine, displacement, cylinders, plant_city, "
            "plant_country, error_code "
            "FROM vin_cache WHERE vin = ?", (vin.upper(),))
        row = self.cur.fetchone()
        if not row:
            return None
        if row["error_code"] == "not_found":
            return None
        return {
            "vin": row["vin"],
            "make": row["make"],
            "model": row["model"],
            "year": row["year"],
            "trim": row["trim"],
            "body_class": row["body_class"],
            "drive_type": row["drive_type"],
            "fuel_type": row["fuel_type"],
            "engine": row["engine"],
            "displacement": row["displacement"],
            "cylinders": row["cylinders"],
            "plant_city": row["plant_city"],
            "plant_country": row["plant_country"],
            "error_code": row["error_code"],
        }

    def upsert_vin_data(self, vin, data):
        """Insert or update VIN decode data."""
        try:
            self.cur.execute("""
                INSERT INTO vin_cache
                    (vin, make, model, year, trim, body_class, drive_type,
                     fuel_type, engine, displacement, cylinders, plant_city,
                     plant_country, error_code, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        CURRENT_TIMESTAMP)
                ON CONFLICT(vin) DO UPDATE SET
                    make = excluded.make, model = excluded.model,
                    year = excluded.year, trim = excluded.trim,
                    body_class = excluded.body_class,
                    drive_type = excluded.drive_type,
                    fuel_type = excluded.fuel_type,
                    engine = excluded.engine,
                    displacement = excluded.displacement,
                    cylinders = excluded.cylinders,
                    plant_city = excluded.plant_city,
                    plant_country = excluded.plant_country,
                    error_code = excluded.error_code,
                    fetched_at = CURRENT_TIMESTAMP
            """, (vin.upper(), data.get("make"), data.get("model"),
                  data.get("year"), data.get("trim"), data.get("body_class"),
                  data.get("drive_type"), data.get("fuel_type"),
                  data.get("engine"), data.get("displacement"),
                  data.get("cylinders"), data.get("plant_city"),
                  data.get("plant_country"), data.get("error_code")))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB VIN upsert error: {e}")

    def update_listing_vin(self, href, vin):
        """Set the VIN for a listing."""
        try:
            self.cur.execute(
                "UPDATE listings SET vin = ?, updated_at = CURRENT_TIMESTAMP "
                "WHERE href = ?", (vin.upper(), href))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB update VIN error: {e}")

    def get_listings_missing_vin(self, limit=50):
        """Get listings that have a description but no VIN yet."""
        self.cur.execute(
            "SELECT id, href, car_name, description FROM listings "
            "WHERE (vin IS NULL OR vin = '') "
            "AND description IS NOT NULL AND description != '' "
            "AND deleted_at IS NULL "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,))
        return self.cur.fetchall()

    def get_vin_data_batch(self, vins):
        """Get cached VIN decode data for multiple VINs at once."""
        if not vins:
            return {}
        vins_upper = [v.upper() for v in vins if v]
        if not vins_upper:
            return {}
        placeholders = ",".join("?" * len(vins_upper))
        self.cur.execute(f"""
            SELECT vin, make, model, year, trim, body_class, drive_type,
                   fuel_type, engine, displacement, cylinders
            FROM vin_cache
            WHERE vin IN ({placeholders}) AND error_code != 'not_found'
        """, vins_upper)
        result = {}
        for row in self.cur.fetchall():
            result[row["vin"]] = {
                "vin": row["vin"],
                "make": row["make"],
                "model": row["model"],
                "year": row["year"],
                "trim": row["trim"],
                "body_class": row["body_class"],
                "drive_type": row["drive_type"],
                "fuel_type": row["fuel_type"],
                "engine": row["engine"],
                "displacement": row["displacement"],
                "cylinders": row["cylinders"],
            }
        return result

    def get_market_prices(self, car_query, year, title_grp="clean"):
        """Get all active listing prices for a car/year/title combo.

        Used to compute percentile-based market value ranges.
        Returns a sorted list of prices.
        """
        if title_grp == "clean":
            # Clean group includes NULL/unknown
            self.cur.execute(
                "SELECT price FROM listings "
                "WHERE car_query = ? AND year = ? AND price IS NOT NULL "
                "AND deleted_at IS NULL "
                "AND (title_type IS NULL OR title_type = '' "
                "     OR title_type = 'clean' OR title_type = 'unknown') "
                "ORDER BY price",
                (car_query, year))
        else:
            self.cur.execute(
                "SELECT price FROM listings "
                "WHERE car_query = ? AND year = ? AND price IS NOT NULL "
                "AND deleted_at IS NULL AND LOWER(title_type) = ? "
                "ORDER BY price",
                (car_query, year, title_grp))
        return [row["price"] for row in self.cur.fetchall()]

    def backfill_vins(self):
        """Extract VINs from existing descriptions and store them."""
        from vin import extract_vin

        rows = self.cur.execute(
            "SELECT id, href, description FROM listings "
            "WHERE (vin IS NULL OR vin = '') "
            "AND description IS NOT NULL AND description != '' "
            "AND deleted_at IS NULL"
        ).fetchall()

        updated = 0
        for row in rows:
            vin = extract_vin(row["description"])
            if vin:
                self.cur.execute(
                    "UPDATE listings SET vin = ? WHERE id = ?",
                    (vin, row["id"]))
                updated += 1

        self.conn.commit()
        if updated:
            logging.info(f"Backfilled VINs for {updated} listings from descriptions.")
        return updated

    def update_vehicle_mpg(self, make, model, year,
                           mpg_city, mpg_highway, mpg_combined):
        """Update just the MPG columns for an existing vehicle_ratings row."""
        try:
            self.cur.execute("""
                UPDATE vehicle_ratings
                SET mpg_city = ?, mpg_highway = ?, mpg_combined = ?
                WHERE make = ? AND model = ? AND year = ?
            """, (mpg_city, mpg_highway, mpg_combined,
                  make.lower(), model.lower(), year))
            if self.cur.rowcount == 0:
                # No existing row — insert a minimal one with just MPG
                self.cur.execute("""
                    INSERT OR IGNORE INTO vehicle_ratings
                        (make, model, year, mpg_city, mpg_highway, mpg_combined,
                         fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (make.lower(), model.lower(), year,
                      mpg_city, mpg_highway, mpg_combined))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB update MPG error: {e}")

    # ── Vehicle Recalls ────────────────────────────────────────────

    def get_vehicle_recalls(self, make, model, year):
        """Get cached recalls for a vehicle. Returns list of dicts."""
        self.cur.execute(
            "SELECT campaign_number, component, summary, consequence, "
            "remedy, report_date, fetched_at "
            "FROM vehicle_recalls "
            "WHERE make = ? AND model = ? AND year = ?",
            (make.lower(), model.lower(), year))
        rows = self.cur.fetchall()
        if not rows:
            return None  # None = never fetched; [] = fetched, no recalls
        return [{
            "campaign_number": r["campaign_number"],
            "component": r["component"],
            "summary": r["summary"],
            "consequence": r["consequence"],
            "remedy": r["remedy"],
            "report_date": r["report_date"],
            "fetched_at": r["fetched_at"],
        } for r in rows]

    def upsert_vehicle_recalls(self, make, model, year, recalls_list):
        """Insert or update recalls for a vehicle (bulk upsert).

        If recalls_list is empty, stores a single sentinel row with
        campaign_number='__none__' so we know we've checked this vehicle.
        """
        make_l = make.lower()
        model_l = model.lower()

        if not recalls_list:
            # Sentinel row — indicates "checked, no recalls found"
            recalls_list = [{
                "campaign_number": "__none__",
                "component": None,
                "summary": None,
                "consequence": None,
                "remedy": None,
                "report_date": None,
            }]

        try:
            for rec in recalls_list:
                self.cur.execute("""
                    INSERT INTO vehicle_recalls
                        (make, model, year, campaign_number, component,
                         summary, consequence, remedy, report_date,
                         fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(make, model, year, campaign_number) DO UPDATE SET
                        component = excluded.component,
                        summary = excluded.summary,
                        consequence = excluded.consequence,
                        remedy = excluded.remedy,
                        report_date = excluded.report_date,
                        fetched_at = CURRENT_TIMESTAMP
                """, (make_l, model_l, year,
                      rec["campaign_number"],
                      rec.get("component"),
                      rec.get("summary"),
                      rec.get("consequence"),
                      rec.get("remedy"),
                      rec.get("report_date")))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB recalls upsert error: {e}")

    def get_all_cached_ratings(self):
        """Get all cached ratings as a dict keyed by (make, model, year)."""
        self.cur.execute(
            "SELECT make, model, year, overall_rating, front_crash_rating, "
            "side_crash_rating, rollover_rating, complaints_count, recalls_count "
            "FROM vehicle_ratings"
        )
        result = {}
        for row in self.cur.fetchall():
            key = (row["make"], row["model"], row["year"])
            result[key] = {
                "overall_rating": row["overall_rating"],
                "front_crash_rating": row["front_crash_rating"],
                "side_crash_rating": row["side_crash_rating"],
                "rollover_rating": row["rollover_rating"],
                "complaints_count": row["complaints_count"] or 0,
                "recalls_count": row["recalls_count"] or 0,
            }
        return result

    # ── Scrape Run Tracking ─────────────────────────────────────────

    def insert_scrape_run(self, source, started_at):
        """Record the start of a scrape run. Returns the run ID."""
        try:
            self.cur.execute(
                "INSERT INTO scrape_runs (source, started_at) VALUES (?, ?)",
                (source, started_at))
            self.conn.commit()
            return self.cur.lastrowid
        except sqlite3.Error as e:
            logging.error(f"DB insert scrape_run error: {e}")
            return None

    def update_scrape_run(self, run_id, **kwargs):
        """Update a scrape run with results."""
        allowed = {"finished_at", "status", "listings_found", "listings_new",
                   "listings_updated", "errors", "error_message",
                   "screenshot_path", "duration_seconds"}
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                vals.append(v)
        if not sets or not run_id:
            return
        vals.append(run_id)
        try:
            self.cur.execute(
                f"UPDATE scrape_runs SET {', '.join(sets)} WHERE id = ?", vals)
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB update scrape_run error: {e}")

    def get_recent_scrape_runs(self, limit=50):
        """Get recent scrape runs, newest first."""
        self.cur.execute(
            "SELECT id, source, started_at, finished_at, status, "
            "listings_found, listings_new, listings_updated, "
            "errors, error_message, screenshot_path, duration_seconds "
            "FROM scrape_runs ORDER BY started_at DESC LIMIT ?",
            (limit,))
        return [dict(row) for row in self.cur.fetchall()]

    def get_scrape_health(self):
        """Compute per-source scrape health by comparing recent runs to historical averages.

        Returns dict: {source: {avg_yield, last_yield, last_status, last_run,
                                 runs_count, health}}
        health is 'good', 'warning', or 'critical'.
        """
        # Get per-source stats from last 30 completed runs
        self.cur.execute("""
            SELECT source,
                   COUNT(*) as run_count,
                   AVG(listings_found) as avg_yield,
                   MAX(started_at) as last_run
            FROM scrape_runs
            WHERE status = 'completed'
            GROUP BY source
        """)
        stats = {}
        for row in self.cur.fetchall():
            stats[row["source"]] = {
                "avg_yield": round(row["avg_yield"] or 0, 1),
                "runs_count": row["run_count"],
                "last_run": row["last_run"],
            }

        # Get latest run per source (any status)
        self.cur.execute("""
            SELECT source, status, listings_found, errors, error_message,
                   started_at, duration_seconds
            FROM scrape_runs
            WHERE id IN (
                SELECT MAX(id) FROM scrape_runs GROUP BY source
            )
        """)
        result = {}
        for row in self.cur.fetchall():
            src = row["source"]
            historical = stats.get(src, {})
            avg_yield = historical.get("avg_yield", 0)
            last_yield = row["listings_found"] or 0

            # Determine health status
            if row["status"] == "failed":
                health = "critical"
            elif avg_yield > 0 and last_yield < avg_yield * 0.2:
                health = "critical"  # < 20% of average
            elif avg_yield > 0 and last_yield < avg_yield * 0.5:
                health = "warning"   # < 50% of average
            elif row["errors"] and row["errors"] > 0:
                health = "warning"
            else:
                health = "good"

            result[src] = {
                "avg_yield": avg_yield,
                "last_yield": last_yield,
                "last_status": row["status"],
                "last_run": row["started_at"],
                "last_duration": row["duration_seconds"],
                "last_errors": row["errors"] or 0,
                "last_error_message": row["error_message"],
                "runs_count": historical.get("runs_count", 0),
                "health": health,
            }

        return result
