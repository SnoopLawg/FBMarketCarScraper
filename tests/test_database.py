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
