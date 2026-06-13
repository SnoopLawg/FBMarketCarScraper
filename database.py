"""Database connection, schema, migration, and CRUD operations."""

import functools
import logging
import os
import sqlite3
import threading
from pathlib import Path

from parsing import (parse_price, parse_mileage, extract_year,
                     detect_powertrain)

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("DATA_DIR", SCRIPT_DIR))
DB_PATH = DATA_DIR / "marketplace_listings.db"


def _synchronized(cls):
    """Serialize every public method on a per-instance reentrant lock.

    web_ui serves on Flask's threaded dev server against ONE shared sqlite
    connection (check_same_thread=False). Two request threads using that same
    connection/cursor at once corrupts cursor state and SEGFAULTS the process
    — which is what made the Analytics page (it fires /api/analytics and
    /api/health in parallel) hang on "Loading…". Holding the lock for the
    duration of each DB call makes access safe. This is a single-user app, so
    serializing quick SQLite ops costs nothing, and long non-DB work (e.g.
    Selenium valuations) lives outside these methods and never holds the lock.
    """
    for name, attr in list(vars(cls).items()):
        if name.startswith("_") or not callable(attr):
            continue

        def make(method):
            @functools.wraps(method)
            def wrapped(self, *args, **kwargs):
                with self._lock:
                    return method(self, *args, **kwargs)
            return wrapped

        setattr(cls, name, make(attr))
    return cls


@_synchronized
class Database:
    # Per-process: paths whose schema has already been migrated this run.
    # Skips the per-open _migrate / _create_tables work after the first
    # connection — they're idempotent but expensive (multi-statement
    # executescript on every new Database() instance).
    _migrated_paths = set()

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        self.conn = None
        self.cur = None
        # Guards the shared connection/cursor against concurrent use by the
        # threaded Flask dev server. Reentrant so a public method may call
        # another public method without deadlocking.
        self._lock = threading.RLock()

    def open(self):
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False,
                                    timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
        path_key = str(self.db_path)
        if path_key not in Database._migrated_paths:
            self._migrate()
            self._create_tables()
            Database._migrated_paths.add(path_key)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cur = None

    # Context-manager support so callers can `with Database() as db:` and the
    # connection is guaranteed to close even on an exception (preventing the
    # `db = Database(); db.open(); ... db.close()` leak pattern).
    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False  # don't swallow

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
                description TEXT,
                vin TEXT,
                enriched_at TEXT,
                owner_count TEXT,
                carfax_url TEXT,
                listed_at TEXT,
                image_urls TEXT,
                is_discovery INTEGER DEFAULT 0,
                seller_type TEXT,
                sold INTEGER DEFAULT 0,
                sold_at TEXT,
                sold_checked_at TEXT,
                drivetrain TEXT,
                powertrain TEXT,
                sold_presumed INTEGER DEFAULT 0
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
                base_msrp REAL,
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

            CREATE TABLE IF NOT EXISTS valuation_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                car_key TEXT NOT NULL,
                source TEXT NOT NULL,
                source_label TEXT,
                private_party_low REAL,
                private_party_high REAL,
                private_party_mid REAL,
                trade_in_value REAL,
                dealer_retail REAL,
                source_url TEXT,
                condition_used TEXT,
                zip_code TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(car_key, source)
            );
            CREATE INDEX IF NOT EXISTS idx_valuation_cache_key
                ON valuation_cache(car_key, source);

            CREATE TABLE IF NOT EXISTS price_trend_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                car_query TEXT NOT NULL,
                year INTEGER NOT NULL,
                title_group TEXT NOT NULL DEFAULT 'all',
                avg_price REAL NOT NULL,
                listing_count INTEGER DEFAULT 0,
                snapshot_date TEXT NOT NULL DEFAULT (date('now')),
                UNIQUE(car_query, year, title_group, snapshot_date)
            );
            CREATE INDEX IF NOT EXISTS idx_price_trend_lookup
                ON price_trend_snapshots(car_query, year, title_group);

            CREATE TABLE IF NOT EXISTS discovery_rotation (
                source TEXT PRIMARY KEY,
                last_index INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
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

        if "is_discovery" not in columns:
            logging.info("Migrating DB: adding is_discovery column...")
            try:
                self.cur.execute(
                    "ALTER TABLE listings ADD COLUMN is_discovery INTEGER DEFAULT 0")
                self.conn.commit()
                logging.info("is_discovery migration complete.")
            except sqlite3.OperationalError:
                pass

        if "seller_type" not in columns:
            logging.info("Migrating DB: adding seller_type column...")
            try:
                self.cur.execute("ALTER TABLE listings ADD COLUMN seller_type TEXT")
                self.conn.commit()
                logging.info("seller_type migration complete.")
            except sqlite3.OperationalError:
                pass

        # Explicit drivetrain from a listing's detail page (e.g. FB's
        # "Drive type: All Wheel Drive"). Overrides the model-default guess
        # at scoring time so AWD/4WD listings aren't mis-scored as FWD.
        if "drivetrain" not in columns:
            logging.info("Migrating DB: adding drivetrain column...")
            try:
                self.cur.execute(
                    "ALTER TABLE listings ADD COLUMN drivetrain TEXT")
                self.conn.commit()
                logging.info("drivetrain migration complete.")
            except sqlite3.OperationalError:
                pass

        # Powertrain (''/hybrid/phev/ev) — splits comp pools so hybrids/EVs
        # aren't priced against the gas version of the same model/year.
        if "powertrain" not in columns:
            logging.info("Migrating DB: adding powertrain column...")
            try:
                self.cur.execute(
                    "ALTER TABLE listings ADD COLUMN powertrain TEXT")
                self.conn.commit()
                logging.info("powertrain migration complete.")
            except sqlite3.OperationalError:
                pass

        # sold_presumed: 1 = inferred-sold from disappearance (weaker comp,
        # weight 3) vs an explicit/confirmed sale (sold=1, presumed=0, weight 8).
        if "sold_presumed" not in columns:
            logging.info("Migrating DB: adding sold_presumed column...")
            try:
                self.cur.execute(
                    "ALTER TABLE listings ADD COLUMN sold_presumed INTEGER DEFAULT 0")
                self.conn.commit()
                logging.info("sold_presumed migration complete.")
            except sqlite3.OperationalError:
                pass

        # base_msrp on vin_cache: original base MSRP from the NHTSA decode
        # (free '% of MSRP retained' depreciation anchor).
        vin_cols = [r[1] for r in self.cur.execute(
            "PRAGMA table_info(vin_cache)").fetchall()]
        if vin_cols and "base_msrp" not in vin_cols:
            logging.info("Migrating DB: adding vin_cache.base_msrp column...")
            try:
                self.cur.execute(
                    "ALTER TABLE vin_cache ADD COLUMN base_msrp REAL")
                self.conn.commit()
                logging.info("base_msrp migration complete.")
            except sqlite3.OperationalError:
                pass

        # Sold tracking: a listing detected as "Sold" on its FB detail page.
        # Its price is the actual market-clearing price — weighted heavily in
        # averages — so sold listings are kept (never stale-deleted).
        if "sold" not in columns:
            logging.info("Migrating DB: adding sold + sold_at columns...")
            try:
                self.cur.execute(
                    "ALTER TABLE listings ADD COLUMN sold INTEGER DEFAULT 0")
                self.cur.execute(
                    "ALTER TABLE listings ADD COLUMN sold_at TEXT")
                self.cur.execute(
                    "ALTER TABLE listings ADD COLUMN sold_checked_at TEXT")
                self.conn.commit()
                logging.info("sold migration complete.")
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
                       owner_count="", carfax_url="", is_discovery=False,
                       seller_type="", vin=""):
        """Insert or update a listing, parsing raw price/mileage/year."""
        href = self._normalize_href(href)

        if deleted_set and href in deleted_set:
            return

        price_val = parse_price(price)
        mileage_val = parse_mileage(mileage_raw)
        year_val = extract_year(car_name)
        # Powertrain from name+trim ('' for gas) — keeps hybrid/EV comps
        # out of the gas pools at scoring time.
        powertrain = detect_powertrain(car_name, trim) or None

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
                     distance, title_type, owner_count, carfax_url,
                     is_discovery, seller_type, vin, powertrain)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    carfax_url = COALESCE(excluded.carfax_url, carfax_url),
                    is_discovery = MIN(is_discovery, excluded.is_discovery),
                    seller_type = COALESCE(excluded.seller_type, seller_type),
                    vin = COALESCE(excluded.vin, vin),
                    powertrain = COALESCE(excluded.powertrain, powertrain)
            """, (href, image_url, price_val, car_name, car_query, location,
                  mileage_val, year_val, source,
                  trim or None, seller or None, condition or None,
                  deal_rating or None, accident_history or None, distance or None,
                  title_type or None, owner_count or None, carfax_url or None,
                  1 if is_discovery else 0, seller_type or None, vin or None,
                  powertrain))
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
            "SELECT price, mileage, year, title_type, vin, sold, powertrain, "
            "sold_presumed "
            "FROM listings "
            "WHERE car_query = ? AND price IS NOT NULL AND year IS NOT NULL "
            "AND deleted_at IS NULL",
            (car_query,)
        )
        return self.cur.fetchall()

    def get_deal_candidates(self, car_query):
        self.cur.execute(
            "SELECT l.href, l.price, l.mileage, l.year, l.location, l.source, "
            "l.image_url, l.car_name, l.created_at, l.updated_at, "
            "l.trim, l.seller, l.condition, l.deal_rating, l.accident_history, "
            "l.distance, l.title_type, l.vin, l.description, l.owner_count, "
            "l.carfax_url, l.listed_at, l.image_urls, l.seller_type, l.sold, "
            "l.sold_at, l.drivetrain, l.powertrain, l.sold_presumed, "
            "vc.base_msrp "
            "FROM listings l "
            "LEFT JOIN vin_cache vc ON vc.vin = l.vin "
            "WHERE l.car_query = ? AND l.price IS NOT NULL "
            "AND l.deleted_at IS NULL",
            (car_query,)
        )
        return [dict(row) for row in self.cur.fetchall()]

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
        """Soft-delete listings from a source that haven't been updated recently.

        Sold listings are exempt: they're our highest-value comparables
        (actual sale prices) and intentionally kept as permanent data points.
        """
        self.cur.execute(
            "UPDATE listings SET deleted_at = CURRENT_TIMESTAMP "
            "WHERE source = ? AND deleted_at IS NULL AND sold = 0 "
            "AND updated_at < datetime('now', ?)",
            (source, f'-{days_old} days'))
        count = self.cur.rowcount
        self.conn.commit()
        return count

    def source_healthy(self, source):
        """Is this source healthy RIGHT NOW? (bot-block guard)

        A presumed-sale only makes sense if the listing's ABSENCE is real —
        not because the scraper is blocked (KSL PerimeterX) and everything
        'disappeared'. Keys off the LATEST run, not a max over a window: KSL
        could have a healthy run yesterday and be failing today (it was), and
        we must NOT presume-sell during the block. The worker records the
        run immediately before calling this, so 'latest' = the just-finished
        scrape. Healthy = latest run completed with yield >= 60% of the
        source's historical average.
        """
        avg = self.cur.execute(
            "SELECT AVG(listings_found) FROM scrape_runs "
            "WHERE source=? AND status='completed' AND listings_found>0 "
            "AND started_at > datetime('now','-30 days')", (source,)).fetchone()[0]
        if not avg:
            return False
        # Latest TERMINAL run (ignore in-progress 'running' rows, and stale
        # ones orphaned when a container restarts mid-scrape).
        latest = self.cur.execute(
            "SELECT status, listings_found FROM scrape_runs "
            "WHERE source=? AND status IN ('completed','failed') "
            "ORDER BY started_at DESC LIMIT 1", (source,)).fetchone()
        if not latest:
            return False
        return (latest["status"] == "completed"
                and (latest["listings_found"] or 0) >= 0.6 * avg)

    def mark_presumed_sold(self, source, min_active_days=4, gone_days=1,
                           grace_days=14):
        """Infer sold-at-last-price for dealer listings that vanished while the
        source was being scraped successfully (Marketcheck-style).

        NOT for Facebook (relisting noise — use its explicit Sold flag). Guards:
          • source_healthy() — absence is real, not a scraper block
          • active >= min_active_days before vanishing (skip quick pulls/blips)
          • not seen for >= gone_days but within grace_days (recent, with context)
          • VIN did NOT reappear as another active listing (skip relists)
        Returns the count marked.
        """
        if source == "facebook" or not self.source_healthy(source):
            return 0
        rows = self.cur.execute(
            """SELECT id, href, vin, price FROM listings
               WHERE source=? AND deleted_at IS NULL AND sold=0
                 AND price IS NOT NULL
                 AND updated_at < datetime('now', ?)
                 AND updated_at > datetime('now', ?)
                 AND julianday(updated_at) - julianday(created_at) >= ?""",
            (source, f'-{gone_days} days', f'-{grace_days} days',
             min_active_days)).fetchall()
        marked = 0
        for r in rows:
            vin = (r["vin"] or "").strip()
            if vin:
                relisted = self.cur.execute(
                    "SELECT 1 FROM listings WHERE upper(vin)=upper(?) "
                    "AND id!=? AND deleted_at IS NULL AND sold=0 LIMIT 1",
                    (vin, r["id"])).fetchone()
                if relisted:
                    continue   # same car still active elsewhere → relisted
            self.cur.execute(
                "UPDATE listings SET sold=1, sold_presumed=1, "
                "sold_at=COALESCE(sold_at, CURRENT_TIMESTAMP) WHERE id=?",
                (r["id"],))
            marked += 1
        self.conn.commit()
        if marked:
            logging.info(f"[{source}] presumed-sold {marked} vanished listings "
                         f"(weighted comps at last asking price).")
        return marked

    def get_analytics_data(self):
        """Return active listings from last 30 days for analytics."""
        self.cur.execute(
            "SELECT car_query, price, mileage, year, source, location, "
            "seller, deal_rating, distance, created_at, "
            "title_type, trim, accident_history, condition, vin "
            "FROM listings WHERE deleted_at IS NULL AND price IS NOT NULL "
            "AND created_at >= datetime('now', '-30 days')"
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

    def get_listings_by_hrefs(self, hrefs, include_deleted=False):
        """Fetch full listing data for a set of hrefs."""
        if not hrefs:
            return []
        placeholders = ",".join("?" * len(hrefs))
        deleted_filter = "" if include_deleted else "AND deleted_at IS NULL"
        self.cur.execute(f"""
            SELECT href, image_url, price, car_name, car_query, location,
                   mileage, year, source, created_at, updated_at,
                   trim, seller, condition, deal_rating, accident_history,
                   distance, title_type, vin, image_urls
            FROM listings
            WHERE href IN ({placeholders}) {deleted_filter}
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

    # ── Price Trend Snapshots ───────────────────────────────────────

    def record_price_snapshot(self, car_query, year, title_group,
                              avg_price, listing_count):
        """Record today's average price for trend tracking."""
        try:
            self.cur.execute("""
                INSERT OR REPLACE INTO price_trend_snapshots
                    (car_query, year, title_group, avg_price,
                     listing_count, snapshot_date)
                VALUES (?, ?, ?, ?, ?, date('now'))
            """, (car_query, year, title_group, avg_price, listing_count))
        except Exception:
            pass  # Don't break averages if snapshot fails

    def backfill_powertrains(self):
        """Classify powertrain for rows that don't have it yet."""
        rows = self.cur.execute(
            "SELECT id, car_name, trim FROM listings "
            "WHERE powertrain IS NULL AND deleted_at IS NULL"
        ).fetchall()
        updated = 0
        for row in rows:
            pt = detect_powertrain(row["car_name"], row["trim"] or "")
            self.cur.execute(
                "UPDATE listings SET powertrain = ? WHERE id = ?",
                (pt, row["id"]))
            if pt:
                updated += 1
        self.conn.commit()
        if rows:
            logging.info(
                f"Powertrain backfill: classified {len(rows)} rows "
                f"({updated} hybrid/phev/ev).")
        return updated

    def propagate_titles_by_vin(self):
        """Copy known titles to unknown listings of the SAME physical car.

        Cross-posted cars (same VIN on Autotrader + Cars.com etc.) often have
        the title disclosed on one source and not the other — e.g. Autotrader
        never states titles inline, but the Cars.com twin's AutoCheck panel
        does. The VIN identifies the physical car, so the known title applies.
        Worst-severity wins when sources disagree (consistent with the
        notes-vs-panel merge rule).
        """
        severity = {"salvage": 0, "rebuilt": 1, "lemon": 2, "clean": 3}
        rows = self.cur.execute("""
            SELECT upper(vin) v, title_type FROM listings
            WHERE deleted_at IS NULL AND vin IS NOT NULL AND vin != ''
              AND title_type IS NOT NULL AND title_type != ''
        """).fetchall()
        best = {}
        for r in rows:
            tt = (r["title_type"] or "").lower()
            if tt not in severity:
                continue
            v = r["v"]
            if v not in best or severity[tt] < severity[best[v]]:
                best[v] = tt
        updated = 0
        unknowns = self.cur.execute("""
            SELECT id, upper(vin) v FROM listings
            WHERE deleted_at IS NULL AND vin IS NOT NULL AND vin != ''
              AND (title_type IS NULL OR title_type = '')
        """).fetchall()
        for r in unknowns:
            tt = best.get(r["v"])
            if tt:
                self.cur.execute(
                    "UPDATE listings SET title_type = ? WHERE id = ?",
                    (tt, r["id"]))
                updated += 1
        self.conn.commit()
        if updated:
            logging.info(f"VIN title propagation: {updated} unknowns resolved "
                         f"from cross-posted twins.")
        return updated

    def get_seller_title_stats(self, sellers):
        """Per-seller branded-title mix from OUR OWN scraped inventory.

        Returns {seller: {known, branded}} where known = listings with a
        resolved title and branded = rebuilt/salvage/lemon among them. Lets
        the buyer playbook flag rebuilt-title specialists (e.g. AutoSavvy)
        when a listing's own title is unknown — data-derived, not a
        hardcoded dealer list.
        """
        sellers = [s for s in set(sellers or []) if s]
        if not sellers:
            return {}
        placeholders = ",".join("?" * len(sellers))
        self.cur.execute(f"""
            SELECT seller,
                   SUM(CASE WHEN title_type IN ('rebuilt','salvage','lemon')
                       THEN 1 ELSE 0 END) AS branded,
                   SUM(CASE WHEN title_type IS NOT NULL AND title_type != ''
                       THEN 1 ELSE 0 END) AS known
            FROM listings
            WHERE seller IN ({placeholders}) AND deleted_at IS NULL
            GROUP BY seller
        """, sellers)
        return {r["seller"]: {"known": r["known"], "branded": r["branded"]}
                for r in self.cur.fetchall()}

    def get_top_car_query(self):
        """The desired-car model with the most active listings."""
        row = self.cur.execute(
            "SELECT car_query, COUNT(*) n FROM listings "
            "WHERE deleted_at IS NULL AND is_discovery = 0 "
            "GROUP BY car_query ORDER BY n DESC LIMIT 1").fetchone()
        return row["car_query"] if row else None

    def get_trend_series(self, car_query, days=90):
        """Daily snapshot series for a car: (date, year, avg_price, count).

        Clean-group only — mixing rebuilt/salvage averages into the same
        line would make the trend jump on composition, not price.
        """
        self.cur.execute("""
            SELECT snapshot_date, year, avg_price, listing_count
            FROM price_trend_snapshots
            WHERE car_query = ? AND title_group = 'clean'
              AND snapshot_date >= date('now', ?)
            ORDER BY snapshot_date ASC, year ASC
        """, (car_query, f'-{days} days'))
        return [dict(r) for r in self.cur.fetchall()]

    def get_price_cut_stats(self, car_query=None, weeks=8):
        """Weekly price-cut counts + average cut size — a market-softness
        signal (more/larger cuts = sellers under pressure = buyer's market).
        """
        q = """
            SELECT strftime('%Y-%W', ph.changed_at) AS wk,
                   COUNT(*) AS cuts,
                   AVG(ph.old_price - ph.new_price) AS avg_cut
            FROM price_history ph
            JOIN listings l ON l.href = ph.listing_href
            WHERE ph.new_price < ph.old_price
              AND ph.changed_at >= datetime('now', ?)
        """
        params = [f'-{weeks * 7} days']
        if car_query:
            q += " AND l.car_query = ?"
            params.append(car_query)
        q += " GROUP BY wk ORDER BY wk ASC"
        self.cur.execute(q, params)
        return [dict(r) for r in self.cur.fetchall()]

    def get_price_trend(self, car_query, year, title_group="clean",
                        days=30):
        """Get price trend for a car/year over the last N days.

        Returns dict with trend direction and magnitude, or None.
        """
        self.cur.execute("""
            SELECT avg_price, listing_count, snapshot_date
            FROM price_trend_snapshots
            WHERE car_query = ? AND year = ? AND title_group = ?
            AND snapshot_date >= date('now', ?)
            ORDER BY snapshot_date ASC
        """, (car_query, year, title_group, f'-{days} days'))
        rows = self.cur.fetchall()
        if len(rows) < 2:
            return None
        first = rows[0]["avg_price"]
        last = rows[-1]["avg_price"]
        if first <= 0:
            return None
        change = last - first
        pct = round(change / first * 100, 1)
        if abs(pct) < 2:
            direction = "stable"
        elif pct > 0:
            direction = "up"
        else:
            direction = "down"
        return {
            "direction": direction,
            "change": round(change),
            "pct": pct,
            "days": (len(rows) - 1),
            "first_price": round(first),
            "last_price": round(last),
        }

    def get_price_trends_batch(self, car_year_groups, days=30):
        """Get trends for multiple (car_query, year, title_group) combos."""
        results = {}
        for car_query, year, grp in car_year_groups:
            key = (car_query, year, grp)
            if key not in results:
                results[key] = self.get_price_trend(
                    car_query, year, grp, days)
        return results

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

    def mark_sold(self, href, sold_price=None):
        """Flag a listing as sold (idempotent — keeps the first sold_at).

        If sold_price is given (the actual final price read from the sold
        detail page), update the stored price to it — sold listings leave
        search so the last search-scraped price may be stale, and this is
        our highest-value comp. The change is logged to price_history.
        """
        try:
            if sold_price is not None:
                row = self.cur.execute(
                    "SELECT price, source FROM listings WHERE href = ?",
                    (href,)).fetchone()
                if row and row["price"] is not None and row["price"] != sold_price:
                    self.cur.execute(
                        "INSERT INTO price_history "
                        "(listing_href, source, old_price, new_price) "
                        "VALUES (?, ?, ?, ?)",
                        (href, row["source"], row["price"], sold_price))
                self.cur.execute(
                    "UPDATE listings SET price = ? WHERE href = ?",
                    (sold_price, href))
            self.cur.execute(
                "UPDATE listings SET sold = 1, "
                "sold_at = COALESCE(sold_at, CURRENT_TIMESTAMP), "
                "sold_checked_at = CURRENT_TIMESTAMP "
                "WHERE href = ?", (href,))
            self.conn.commit()
            return self.cur.rowcount
        except sqlite3.Error as e:
            logging.error(f"DB mark_sold error: {e}")
            return 0

    def mark_sold_checked(self, href):
        """Record that we checked a listing's sold status (no change)."""
        try:
            self.cur.execute(
                "UPDATE listings SET sold_checked_at = CURRENT_TIMESTAMP "
                "WHERE href = ?", (href,))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB mark_sold_checked error: {e}")

    def get_enriched_hrefs(self, source):
        """Set of active hrefs already enriched (detail page visited) — the
        inline FB scrape skips a detail visit for these and just refreshes
        their price from the search card."""
        self.cur.execute(
            "SELECT href FROM listings WHERE source = ? "
            "AND enriched_at IS NOT NULL AND deleted_at IS NULL", (source,))
        return {r["href"] for r in self.cur.fetchall()}

    def get_active_listings_for_sold_check(self, source, limit=60):
        """Active listings to re-visit for a sold check, least-recently
        checked first. Sold listings drop out of FB search, so the only way
        to catch a sale is to re-visit known-active detail pages."""
        self.cur.execute(
            "SELECT id, href, car_name FROM listings "
            "WHERE source = ? AND deleted_at IS NULL AND sold = 0 "
            "ORDER BY sold_checked_at IS NOT NULL, sold_checked_at ASC, "
            "updated_at DESC LIMIT ?",
            (source, limit))
        return self.cur.fetchall()

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
                   "image_urls", "seller_type", "sold", "drivetrain",
                   "owner_count"}
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
        """Scan car_name + description for title keywords and backfill title_type."""
        updated = 0
        rows = self.cur.execute(
            "SELECT id, car_name, description FROM listings "
            "WHERE (title_type IS NULL OR title_type = '') "
            "AND deleted_at IS NULL"
        ).fetchall()
        for row in rows:
            text = f"{row['car_name'] or ''} {row['description'] or ''}".lower()
            title_type = None
            if "salvage" in text:
                title_type = "salvage"
            elif "rebuilt" in text or "reconstructed" in text or "r/r title" in text:
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
            logging.info(f"Backfilled title_type for {updated} listings from car_name/description text.")
        return updated

    def backfill_owner_counts(self):
        """Parse owner count from descriptions and persist to DB."""
        from parsing import parse_owner_count
        updated = 0
        rows = self.cur.execute(
            "SELECT id, car_name, description FROM listings "
            "WHERE owner_count IS NULL AND description IS NOT NULL "
            "AND deleted_at IS NULL"
        ).fetchall()
        for row in rows:
            combined = f"{row['car_name'] or ''} {row['description'] or ''}"
            count = parse_owner_count(combined)
            if count is not None:
                self.cur.execute(
                    "UPDATE listings SET owner_count = ? WHERE id = ?",
                    (count, row["id"]))
                updated += 1
        self.conn.commit()
        if updated:
            logging.info(f"Backfilled owner_count for {updated} listings from descriptions.")
        return updated

    def backfill_seller_types(self):
        """Classify seller_type from existing seller name, href, and source."""
        from parsing import classify_seller_type
        updated = 0
        rows = self.cur.execute(
            "SELECT id, seller, href, source, description FROM listings "
            "WHERE (seller_type IS NULL OR seller_type = '') "
            "AND deleted_at IS NULL"
        ).fetchall()
        for row in rows:
            st = classify_seller_type(
                seller_name=row["seller"],
                href=row["href"],
                source=row["source"],
                description=row["description"],
            )
            if st:
                self.cur.execute(
                    "UPDATE listings SET seller_type = ? WHERE id = ?",
                    (st, row["id"]))
                updated += 1
        self.conn.commit()
        if updated:
            logging.info(f"Backfilled seller_type for {updated} listings.")
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
                     plant_country, base_msrp, error_code, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
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
                    base_msrp = excluded.base_msrp,
                    error_code = excluded.error_code,
                    fetched_at = CURRENT_TIMESTAMP
            """, (vin.upper(), data.get("make"), data.get("model"),
                  data.get("year"), data.get("trim"), data.get("body_class"),
                  data.get("drive_type"), data.get("fuel_type"),
                  data.get("engine"), data.get("displacement"),
                  data.get("cylinders"), data.get("plant_city"),
                  data.get("plant_country"), data.get("base_msrp"),
                  data.get("error_code")))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB VIN upsert error: {e}")

    def backfill_base_msrp(self, limit=5000):
        """Populate vin_cache.base_msrp for ACTIVE listings' VINs missing it.

        Targets VINs on live listings (not the 30k+ historical vin_cache rows,
        most of which are old/undecodable) so coverage actually moves. New
        decodes capture base_msrp automatically; this re-decodes the active
        backlog (batched, free NHTSA). Returns the count populated.
        """
        from vin import decode_vins_batch
        rows = self.cur.execute(
            "SELECT DISTINCT v.vin FROM vin_cache v "
            "JOIN listings l ON l.vin = v.vin AND l.deleted_at IS NULL "
            "WHERE v.base_msrp IS NULL "
            "AND (v.error_code IS NULL OR v.error_code LIKE '0%' "
            "OR v.error_code LIKE '1%') LIMIT ?", (limit,)).fetchall()
        vins = [r["vin"] for r in rows]
        if not vins:
            return 0
        decoded = decode_vins_batch(vins)   # chunks 50/call internally
        n = 0
        for vin, data in decoded.items():
            if data and data.get("base_msrp"):
                self.cur.execute(
                    "UPDATE vin_cache SET base_msrp = ? WHERE vin = ?",
                    (data["base_msrp"], vin))
                n += 1
        self.conn.commit()
        if n:
            logging.info(f"[base_msrp] backfilled {n}/{len(vins)} active VINs")
        return n

    def get_fb_listings_missing_mileage(self, limit=40):
        """Active FB listings with no mileage — the pre-fix backlog (FB cards
        stopped showing mileage). A hidden odometer can mint a false deal
        (inflated estimate + flattered mileage factor), so these get a one-time
        detail-page mileage backfill. Newest first (likeliest still active)."""
        self.cur.execute(
            "SELECT id, href FROM listings WHERE source = 'facebook' "
            "AND deleted_at IS NULL AND sold = 0 "
            "AND (mileage IS NULL OR mileage = 0) "
            "ORDER BY created_at DESC LIMIT ?", (limit,))
        return self.cur.fetchall()

    def update_listing_mileage(self, href, mileage):
        """Set a listing's mileage (used by the FB mileage backfill)."""
        try:
            self.cur.execute(
                "UPDATE listings SET mileage = ? WHERE href = ?",
                (mileage, href))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB update mileage error: {e}")

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
        """Active listing prices for a car/year/title(+powertrain) combo.

        title_grp may carry a powertrain suffix ("clean#hybrid") so hybrid/
        EV ranges only include same-powertrain comps; the plain group means
        gas/unspecified. Returns a sorted list of prices.
        """
        grp, _, powertrain = title_grp.partition("#")
        if powertrain:
            pt_clause = "AND LOWER(COALESCE(powertrain, '')) = ? "
            pt_params = [powertrain]
        else:
            pt_clause = "AND (powertrain IS NULL OR powertrain = '') "
            pt_params = []
        if grp == "clean":
            # Clean group includes NULL/unknown
            self.cur.execute(
                "SELECT price FROM listings "
                "WHERE car_query = ? AND year = ? AND price IS NOT NULL "
                "AND deleted_at IS NULL "
                "AND (title_type IS NULL OR title_type = '' "
                "     OR title_type = 'clean' OR title_type = 'unknown') "
                + pt_clause + "ORDER BY price",
                (car_query, year, *pt_params))
        else:
            self.cur.execute(
                "SELECT price FROM listings "
                "WHERE car_query = ? AND year = ? AND price IS NOT NULL "
                "AND deleted_at IS NULL AND LOWER(title_type) = ? "
                + pt_clause + "ORDER BY price",
                (car_query, year, grp, *pt_params))
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

    # ── Valuation Cache ──────────────────────────────────────────────

    def get_cached_valuations(self, car_key):
        """Get all cached valuations for a car_key. Returns list of Row or []."""
        self.cur.execute(
            "SELECT source, source_label, private_party_low, private_party_high, "
            "private_party_mid, trade_in_value, dealer_retail, source_url, "
            "condition_used, zip_code, fetched_at "
            "FROM valuation_cache WHERE car_key = ?",
            (car_key,))
        return self.cur.fetchall()

    def upsert_valuation(self, car_key, source, source_label=None,
                         private_party_low=None, private_party_high=None,
                         private_party_mid=None, trade_in_value=None,
                         dealer_retail=None, source_url=None,
                         condition_used=None, zip_code=None):
        """Insert or update a cached valuation."""
        try:
            self.cur.execute("""
                INSERT INTO valuation_cache
                    (car_key, source, source_label, private_party_low,
                     private_party_high, private_party_mid, trade_in_value,
                     dealer_retail, source_url, condition_used, zip_code,
                     fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(car_key, source) DO UPDATE SET
                    source_label = excluded.source_label,
                    private_party_low = excluded.private_party_low,
                    private_party_high = excluded.private_party_high,
                    private_party_mid = excluded.private_party_mid,
                    trade_in_value = excluded.trade_in_value,
                    dealer_retail = excluded.dealer_retail,
                    source_url = excluded.source_url,
                    condition_used = excluded.condition_used,
                    zip_code = excluded.zip_code,
                    fetched_at = CURRENT_TIMESTAMP
            """, (car_key, source, source_label, private_party_low,
                  private_party_high, private_party_mid, trade_in_value,
                  dealer_retail, source_url, condition_used, zip_code))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB valuation upsert error: {e}")

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

    def get_daily_yield(self, days=7):
        """Per-source, per-day scrape yield for the last N days."""
        self.cur.execute("""
            SELECT source,
                   date(started_at) AS day,
                   COUNT(*) AS runs,
                   SUM(listings_found) AS listings_found,
                   SUM(errors) AS errors
            FROM scrape_runs
            WHERE started_at >= date('now', ?)
            GROUP BY source, day
            ORDER BY source, day
        """, (f"-{int(days)} day",))
        return [dict(row) for row in self.cur.fetchall()]

    def get_data_quality(self):
        """Per-source data-quality stats over active (non-deleted) listings."""
        self.cur.execute("""
            SELECT source,
                   COUNT(*) AS active,
                   SUM(price IS NULL OR price = 0) AS missing_price,
                   SUM(mileage IS NULL) AS missing_mileage,
                   SUM(year IS NULL) AS missing_year,
                   SUM(price IS NOT NULL AND price > 0 AND price < 500)
                       AS price_under_500,
                   SUM(price > 100000) AS price_over_100k
            FROM listings
            WHERE deleted_at IS NULL
            GROUP BY source
        """)
        return [dict(row) for row in self.cur.fetchall()]

    def get_new_listing_counts(self):
        """New listings per source: last 24h vs the 24h before that."""
        self.cur.execute("""
            SELECT source,
                   SUM(created_at >= datetime('now', '-1 day')) AS last_24h,
                   SUM(created_at >= datetime('now', '-2 day')
                       AND created_at < datetime('now', '-1 day')) AS prev_24h
            FROM listings
            WHERE created_at >= datetime('now', '-2 day')
            GROUP BY source
        """)
        return [dict(row) for row in self.cur.fetchall()]

    def get_listing_totals(self):
        """Overall listing totals and data date range."""
        self.cur.execute("""
            SELECT COUNT(*) AS total,
                   SUM(deleted_at IS NULL) AS active,
                   MIN(created_at) AS first_created,
                   MAX(updated_at) AS last_updated
            FROM listings
        """)
        return dict(self.cur.fetchone())

    # ── Discovery Rotation ────────────────────────────────────────

    def get_rotation_index(self, source):
        """Return the last rotation index for a source."""
        self.cur.execute(
            "SELECT last_index FROM discovery_rotation WHERE source = ?",
            (source,))
        row = self.cur.fetchone()
        return row["last_index"] if row else 0

    def update_rotation_index(self, source, new_index):
        """Upsert the rotation index for a source."""
        try:
            self.cur.execute("""
                INSERT INTO discovery_rotation (source, last_index, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(source) DO UPDATE SET
                    last_index = excluded.last_index,
                    updated_at = CURRENT_TIMESTAMP
            """, (source, new_index))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB update rotation index error: {e}")

    def has_listings_for_query(self, car_query):
        """Check if any non-deleted listings exist for a car query."""
        self.cur.execute(
            "SELECT 1 FROM listings "
            "WHERE car_query = ? AND deleted_at IS NULL LIMIT 1",
            (car_query,))
        return self.cur.fetchone() is not None
