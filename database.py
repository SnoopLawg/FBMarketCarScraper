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
                distance TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_listings_href_source ON listings(href, source);
            CREATE INDEX IF NOT EXISTS idx_listings_car_query ON listings(car_query);
            CREATE INDEX IF NOT EXISTS idx_listings_href ON listings(href);
            CREATE INDEX IF NOT EXISTS idx_listings_source ON listings(source);

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

    # ── Inserts ────────────────────────────────────────────────────

    def insert_listing(self, *, car_query, href, image_url, price, car_name,
                       location, mileage_raw, source, deleted_set=None,
                       trim="", seller="", condition="", deal_rating="",
                       accident_history="", distance=""):
        """Insert or update a listing, parsing raw price/mileage/year."""
        if deleted_set and href in deleted_set:
            return

        price_val = parse_price(price)
        mileage_val = parse_mileage(mileage_raw)
        year_val = extract_year(car_name)

        try:
            self.cur.execute("""
                INSERT INTO listings
                    (href, image_url, price, car_name, car_query, location,
                     mileage, year, source, updated_at,
                     trim, seller, condition, deal_rating, accident_history, distance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP,
                        ?, ?, ?, ?, ?, ?)
                ON CONFLICT(href, source) DO UPDATE SET
                    price = excluded.price,
                    image_url = COALESCE(excluded.image_url, image_url),
                    updated_at = CURRENT_TIMESTAMP,
                    trim = COALESCE(excluded.trim, trim),
                    seller = COALESCE(excluded.seller, seller),
                    deal_rating = COALESCE(excluded.deal_rating, deal_rating),
                    accident_history = COALESCE(excluded.accident_history, accident_history),
                    distance = COALESCE(excluded.distance, distance)
            """, (href, image_url, price_val, car_name, car_query, location,
                  mileage_val, year_val, source,
                  trim or None, seller or None, condition or None,
                  deal_rating or None, accident_history or None, distance or None))
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
            "SELECT price, mileage, year FROM listings "
            "WHERE car_query = ? AND price IS NOT NULL AND year IS NOT NULL",
            (car_query,)
        )
        return self.cur.fetchall()

    def get_deal_candidates(self, car_query):
        self.cur.execute(
            "SELECT href, price, mileage, year, location, source, "
            "image_url, car_name, created_at, updated_at, "
            "trim, seller, condition, deal_rating, accident_history, distance "
            "FROM listings "
            "WHERE car_query = ? AND price IS NOT NULL AND deleted_at IS NULL",
            (car_query,)
        )
        return self.cur.fetchall()

    def get_averages(self, car_query):
        self.cur.execute(
            "SELECT year, avg_lower_mileage_price, avg_higher_mileage_price "
            "FROM average_prices WHERE car_query = ?",
            (car_query,)
        )
        return {row[0]: (row[1], row[2]) for row in self.cur.fetchall()}

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
        """Return all computed averages for analytics."""
        self.cur.execute(
            "SELECT car_query, year, avg_lower_mileage_price, avg_higher_mileage_price "
            "FROM average_prices"
        )
        return self.cur.fetchall()

    # ── Averages ───────────────────────────────────────────────────

    def upsert_average(self, car_query, year, avg_lower, avg_higher):
        self.cur.execute("""
            INSERT OR REPLACE INTO average_prices
                (car_query, year, avg_lower_mileage_price, avg_higher_mileage_price)
            VALUES (?, ?, ?, ?)
        """, (car_query, year, avg_lower, avg_higher))
        self.conn.commit()
