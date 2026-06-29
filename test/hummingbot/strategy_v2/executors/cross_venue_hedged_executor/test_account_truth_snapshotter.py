import sqlite3
import threading
from decimal import Decimal

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.account_truth_snapshotter import (
    AccountTruthSnapshotter,
)


def _snap(ts, qty="9"):
    return {
        "account_id": "A",
        "venue": "kis",
        "snapshot_ts": ts,
        "source": "t",
        "positions": {
            "005930": {
                "qty": Decimal(qty),
                "avg_entry": Decimal("1"),
                "mark": Decimal("2"),
                "ccy": "KRW",
            }
        },
        "cash_by_ccy": {"KRW": Decimal("100")},
    }


def test_enqueue_is_nonblocking_and_writes_off_thread(tmp_path):
    db = tmp_path / "_truth" / "account_truth.sqlite"
    s = AccountTruthSnapshotter(db_path=str(db))
    caller = threading.get_ident()
    s.enqueue(_snap(1.0))
    s.flush(timeout=2.0)
    rows = sqlite3.connect(str(db)).execute(
        "SELECT account_id, venue, snapshot_ts FROM account_truth_snapshot"
    ).fetchall()
    assert rows and rows[0][2] == 1.0
    assert s.writer_thread_ident != caller
    s.close()


def test_enqueue_deep_copies_payload(tmp_path):
    s = AccountTruthSnapshotter(db_path=str(tmp_path / "t.sqlite"))
    payload = _snap(1.0)
    s.enqueue(payload)
    payload["positions"]["005930"]["qty"] = Decimal("999")
    s.flush(timeout=2.0)
    raw = sqlite3.connect(str(tmp_path / "t.sqlite")).execute(
        "SELECT positions FROM account_truth_snapshot"
    ).fetchone()[0]
    assert '"999"' not in raw
    s.close()


def test_bounded_queue_drops_oldest(tmp_path):
    s = AccountTruthSnapshotter(db_path=str(tmp_path / "t.sqlite"), queue_maxsize=2, _start_worker=False)
    for ts in (1.0, 2.0, 3.0, 4.0):
        s.enqueue(_snap(ts))
    kept = sorted(item["snapshot_ts"] for item in s.drain_queue_for_test())
    assert kept == [3.0, 4.0]


def test_atomic_upsert_no_torn_read(tmp_path):
    s = AccountTruthSnapshotter(db_path=str(tmp_path / "t.sqlite"))
    s.enqueue(_snap(1.0, qty="9"))
    s.flush(2.0)
    s.enqueue(_snap(2.0, qty="7"))
    s.flush(2.0)
    rows = sqlite3.connect(str(tmp_path / "t.sqlite")).execute(
        "SELECT snapshot_ts FROM account_truth_snapshot WHERE account_id='A' AND venue='kis'"
    ).fetchall()
    assert len(rows) == 1 and rows[0][0] == 2.0
    s.close()


def test_write_error_does_not_kill_worker(tmp_path):
    s = AccountTruthSnapshotter(db_path=str(tmp_path / "t.sqlite"))
    bad = _snap(1.0)
    bad["positions"] = object()
    s.enqueue(bad)
    s.flush(2.0)
    s.enqueue(_snap(2.0, qty="7"))
    s.flush(2.0)
    assert s._thread.is_alive()
    rows = sqlite3.connect(str(tmp_path / "t.sqlite")).execute(
        "SELECT snapshot_ts FROM account_truth_snapshot"
    ).fetchall()
    assert rows and rows[0][0] == 2.0
    s.close()
