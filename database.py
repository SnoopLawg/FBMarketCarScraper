"""Database connection, schema, migration, and CRUD operations."""

import logging
import sqlite3
from pathlib import Path

from parsing import parse_price, parse_mileage, extract_year

SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR / "marketplace_listings.db"


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
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(make, model, year)
            );
            CREATE INDEX IF NOT EXISTS idx_vehicle_ratings_lookup
                ON vehicle_ratings(make, model, year);
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

    def insert_listing(self, *, car_query, href, image_url, price, car_name,
                       location, mileage_raw, source, deleted_set=None,
                       trim="", seller="", condition="", deal_rating="",
                       accident_history="", distance="", title_type=""):
        """Insert or update a listing, parsing raw price/mileage/year."""
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
                     distance, title_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP,
                        ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(href, source) DO UPDATE SET
                    price = excluded.price,
                    image_url = COALESCE(excluded.image_url, image_url),
                    updated_at = CURRENT_TIMESTAMP,
                    trim = COALESCE(excluded.trim, trim),
                    seller = COALESCE(excluded.seller, seller),
                    deal_rating = COALESCE(excluded.deal_rating, deal_rating),
                    accident_history = COALESCE(excluded.accident_history, accident_history),
                    distance = COALESCE(excluded.distance, distance),
                    title_type = COALESCE(excluded.title_type, title_type)
            """, (href, image_url, price_val, car_name, car_query, location,
                  mileage_val, year_val, source,
                  trim or None, seller or None, condition or None,
                  deal_rating or None, accident_history or None, distance or None,
                  title_type or None))
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
            "title_type "
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
            "seller, deal_rating, distance, created_at "
            "FROM listings WHERE deleted_at IS NULL AND price IS NOT NULL"
        )
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
                   distance, title_type
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
            "rollover_rating, complaints_count, recalls_count, fetched_at "
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
            "fetched_at": row["fetched_at"],
        }

    def upsert_vehicle_rating(self, *, make, model, year,
                               overall_rating, front_crash, side_crash,
                               rollover, complaints, recalls):
        """Insert or update a cached NHTSA rating."""
        try:
            self.cur.execute("""
                INSERT INTO vehicle_ratings
                    (make, model, year, overall_rating, front_crash_rating,
                     side_crash_rating, rollover_rating,
                     complaints_count, recalls_count, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(make, model, year) DO UPDATE SET
                    overall_rating = excluded.overall_rating,
                    front_crash_rating = excluded.front_crash_rating,
                    side_crash_rating = excluded.side_crash_rating,
                    rollover_rating = excluded.rollover_rating,
                    complaints_count = excluded.complaints_count,
                    recalls_count = excluded.recalls_count,
                    fetched_at = CURRENT_TIMESTAMP
            """, (make.lower(), model.lower(), year,
                  overall_rating, front_crash, side_crash,
                  rollover, complaints, recalls))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB vehicle rating upsert error: {e}")

    def update_title_type(self, href, title_type):
        """Update the title_type for a specific listing."""
        try:
            self.cur.execute(
                "UPDATE listings SET title_type = ?, updated_at = CURRENT_TIMESTAMP "
                "WHERE href = ?", (title_type, href))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB update title_type error: {e}")

    def update_listing_details(self, href, **kwargs):
        """Update multiple detail fields for a listing."""
        allowed = {"title_type", "trim", "seller", "condition",
                   "deal_rating", "accident_history", "description"}
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k in allowed and v:
                sets.append(f"{k} = ?")
                vals.append(v)
        if not sets:
            return
        sets.append("updated_at = CURRENT_TIMESTAMP")
        vals.append(href)
        try:
            self.cur.execute(
                f"UPDATE listings SET {', '.join(sets)} WHERE href = ?", vals)
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB update details error: {e}")

    def get_listings_missing_title_type(self, source=None, limit=50):
        """Get listings that need enrichment.

        Prioritizes: missing title_type first, then missing description.
        This way we always fill in the critical scoring data first,
        and backfill descriptions with any remaining capacity.
        """
        q = ("SELECT id, href, car_name FROM listings "
             "WHERE ((title_type IS NULL OR title_type = '') "
             "       OR (description IS NULL OR description = '')) "
             "AND deleted_at IS NULL")
        params = []
        if source:
            q += " AND source = ?"
            params.append(source)
        # Sort: missing title_type first (most important), then missing description
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
