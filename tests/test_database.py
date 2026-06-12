"""Tests for Database connection lifecycle and migration gating."""
from unittest.mock import patch

import pytest

from database import Database


# ── Context manager ───────────────────────────────────────────────


def test_with_block_opens_and_closes(tmp_path):
    path = tmp_path / "t.db"
    db = Database(db_path=path)
    assert db.conn is None
    with db:
        assert db.conn is not None
        # writable
        db.cur.execute("INSERT INTO listings (href, source) VALUES (?, ?)",
                       ("https://x/1", "test"))
        db.conn.commit()
    assert db.conn is None  # closed after with


def test_with_block_closes_on_exception(tmp_path):
    db = Database(db_path=tmp_path / "t.db")
    with pytest.raises(RuntimeError):
        with db:
            assert db.conn is not None
            raise RuntimeError("boom")
    assert db.conn is None, "connection must close even on exception"


def test_context_manager_does_not_swallow_exception(tmp_path):
    """__exit__ returns False so exceptions propagate normally."""
    with pytest.raises(ValueError, match="propagate"):
        with Database(db_path=tmp_path / "t.db"):
            raise ValueError("propagate me")


# ── Migrations gated per-path ─────────────────────────────────────


def test_migrate_runs_once_per_db_path(tmp_path):
    """Reopening the same DB in the same process must not re-run migrations
    (they're idempotent but expensive)."""
    path = tmp_path / "once.db"
    Database._migrated_paths.discard(str(path))  # clean slate

    with patch.object(Database, "_migrate", autospec=True) as m_mig, \
         patch.object(Database, "_create_tables", autospec=True) as m_tab:
        with Database(db_path=path):
            pass
        with Database(db_path=path):
            pass
        with Database(db_path=path):
            pass
    assert m_mig.call_count == 1, "migrate must run exactly once per path"
    assert m_tab.call_count == 1, "_create_tables must run exactly once"


def test_different_paths_each_migrate(tmp_path):
    """Different DB paths each get their own one-time migration."""
    p1, p2 = tmp_path / "a.db", tmp_path / "b.db"
    Database._migrated_paths.discard(str(p1))
    Database._migrated_paths.discard(str(p2))

    with patch.object(Database, "_migrate", autospec=True) as m_mig:
        with Database(db_path=p1):
            pass
        with Database(db_path=p2):
            pass
    assert m_mig.call_count == 2


def test_open_close_manual_pattern_still_works(tmp_path):
    """Backward-compat: the explicit open()/close() pattern must keep working
    for callers that haven't been migrated to `with`."""
    db = Database(db_path=tmp_path / "manual.db")
    db.open()
    try:
        assert db.cur is not None
        n = db.cur.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        assert n == 0
    finally:
        db.close()
    assert db.conn is None


# ── Sold marking + price capture ─────────────────────────────────

def _insert(db, href, price, source="facebook", vin="", title_type=""):
    db.insert_listing(
        car_query="Ford Escape", href=href, image_url="x", price=str(price),
        car_name="2017 Ford Escape Titanium", location="Bluffdale, UT",
        mileage_raw="107000 miles", source=source, vin=vin,
        title_type=title_type)


def test_mark_sold_sets_flag_and_keeps_price_when_no_price_given(tmp_path):
    with Database(db_path=tmp_path / "s.db") as db:
        _insert(db, "h1", 6000)
        db.mark_sold("h1")
        row = db.cur.execute(
            "SELECT sold, sold_at, price FROM listings WHERE href='h1'").fetchone()
        assert row["sold"] == 1 and row["sold_at"] and row["price"] == 6000


def test_mark_sold_updates_price_to_actual_sale_price(tmp_path):
    """Seller dropped to $5,500 right before selling; our last search scrape
    had $6,000. mark_sold with the detail-page price must correct it and log
    the change to price_history."""
    with Database(db_path=tmp_path / "s.db") as db:
        _insert(db, "h2", 6000)
        db.mark_sold("h2", sold_price=5500)
        row = db.cur.execute(
            "SELECT sold, price FROM listings WHERE href='h2'").fetchone()
        assert row["sold"] == 1 and row["price"] == 5500
        hist = db.cur.execute(
            "SELECT old_price, new_price FROM price_history "
            "WHERE listing_href='h2'").fetchone()
        assert hist["old_price"] == 6000 and hist["new_price"] == 5500


def test_mark_sold_idempotent_keeps_first_sold_at(tmp_path):
    with Database(db_path=tmp_path / "s.db") as db:
        _insert(db, "h3", 5500)
        db.mark_sold("h3", sold_price=5500)
        first = db.cur.execute(
            "SELECT sold_at FROM listings WHERE href='h3'").fetchone()["sold_at"]
        db.mark_sold("h3", sold_price=5500)
        again = db.cur.execute(
            "SELECT sold_at FROM listings WHERE href='h3'").fetchone()["sold_at"]
        assert first == again  # sold_at preserved on re-mark


def test_propagate_titles_by_vin_copies_known_to_unknown(tmp_path):
    with Database(db_path=tmp_path / "p.db") as db:
        # Same VIN, two sources: Cars.com knows rebuilt, Autotrader unknown
        _insert(db, "https://carscom/1", 20000, source="carscom",
                vin="1FMCU9GN5PUB20446", title_type="rebuilt")
        _insert(db, "https://autotrader/1", 20000, source="autotrader",
                vin="1FMCU9GN5PUB20446")
        n = db.propagate_titles_by_vin()
        assert n == 1
        row = db.cur.execute(
            "SELECT title_type FROM listings WHERE href='https://autotrader/1'"
        ).fetchone()
        assert row["title_type"] == "rebuilt"


def test_propagate_titles_worst_severity_wins(tmp_path):
    with Database(db_path=tmp_path / "p2.db") as db:
        _insert(db, "https://a/1", 20000, source="carscom",
                vin="VINSHARED0000001", title_type="clean")
        _insert(db, "https://a/2", 20000, source="ksl",
                vin="VINSHARED0000001", title_type="salvage")
        _insert(db, "https://a/3", 20000, source="autotrader",
                vin="VINSHARED0000001")
        db.propagate_titles_by_vin()
        row = db.cur.execute(
            "SELECT title_type FROM listings WHERE href='https://a/3'").fetchone()
        assert row["title_type"] == "salvage"  # worst wins


def _seed_run(db, source, found, status="completed", days_ago=0):
    db.cur.execute(
        "INSERT INTO scrape_runs (source, started_at, status, listings_found) "
        "VALUES (?, datetime('now', ?), ?, ?)",
        (source, f"-{days_ago} days", status, found))
    db.conn.commit()


def _seed_listing(db, href, source, vin, created_days, updated_days, price=15000):
    db.insert_listing(car_query="Toyota RAV4", href=href, image_url="x",
                      price=str(price), car_name="2019 Toyota RAV4 LE",
                      location="UT", mileage_raw="60000 miles", source=source,
                      vin=vin)
    db.cur.execute(
        "UPDATE listings SET created_at=datetime('now', ?), "
        "updated_at=datetime('now', ?) WHERE href=?",
        (f"-{created_days} days", f"-{updated_days} days", href))
    db.conn.commit()


def test_presumed_sold_marks_vanished_dealer_listing(tmp_path):
    with Database(db_path=tmp_path / "ps.db") as db:
        _seed_run(db, "carscom", 400)                     # healthy
        # active 9 days, last seen 2 days ago → vanished during healthy scrape
        _seed_listing(db, "https://cars.com/a", "carscom", "VIN0000000000001", 9, 2)
        n = db.mark_presumed_sold("carscom")
        assert n == 1
        row = db.cur.execute("SELECT sold, sold_presumed FROM listings "
                             "WHERE href='https://cars.com/a'").fetchone()
        assert row["sold"] == 1 and row["sold_presumed"] == 1


def test_presumed_sold_skips_when_source_blocked(tmp_path):
    # The KSL lesson: a blocked source has 0/failed runs → don't presume sold.
    with Database(db_path=tmp_path / "ps2.db") as db:
        _seed_run(db, "ksl", 200, days_ago=20)            # healthy long ago
        _seed_run(db, "ksl", 0, status="failed")          # blocked now
        _seed_listing(db, "https://ksl/a", "ksl", "VIN0000000000002", 9, 2)
        assert db.mark_presumed_sold("ksl") == 0


def test_presumed_sold_skips_facebook(tmp_path):
    with Database(db_path=tmp_path / "ps3.db") as db:
        _seed_run(db, "facebook", 300)
        _seed_listing(db, "https://fb/a", "facebook", "VIN0000000000003", 9, 2)
        assert db.mark_presumed_sold("facebook") == 0


def test_presumed_sold_skips_relisted_vin(tmp_path):
    with Database(db_path=tmp_path / "ps4.db") as db:
        _seed_run(db, "carscom", 400)
        _seed_listing(db, "https://cars.com/old", "carscom", "VINRELIST0000001", 9, 2)
        # same VIN reappeared as a fresh active listing → relisted, not sold
        _seed_listing(db, "https://cars.com/new", "carscom", "VINRELIST0000001", 1, 0)
        assert db.mark_presumed_sold("carscom") == 0


def test_presumed_sold_skips_freshly_listed(tmp_path):
    with Database(db_path=tmp_path / "ps5.db") as db:
        _seed_run(db, "carscom", 400)
        # only active 1 day before vanishing → too quick, likely a pull/blip
        _seed_listing(db, "https://cars.com/fresh", "carscom", "VIN0000000000005", 1.2, 2)
        assert db.mark_presumed_sold("carscom") == 0
