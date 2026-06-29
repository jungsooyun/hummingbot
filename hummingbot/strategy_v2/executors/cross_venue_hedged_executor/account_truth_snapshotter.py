import copy
import json
import os
import queue
import sqlite3
import threading
from decimal import Decimal


_SENTINEL = object()


def _json_default(o):
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


class AccountTruthSnapshotter:
    """Off-loop writer for account-truth snapshots.

    The trading loop only calls enqueue(): non-blocking, bounded, drop-oldest,
    and never raises into the caller. The daemon owns SQLite writes.
    """

    def __init__(self, db_path: str, *, queue_maxsize: int = 64, _start_worker: bool = True):
        self._db_path = db_path
        self._queue: queue.Queue = queue.Queue(maxsize=queue_maxsize)
        self._closed = False
        self.writer_thread_ident = None
        self._drained = threading.Event()
        self._drained.set()
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._init_schema()
        self._thread = None
        if _start_worker:
            self._thread = threading.Thread(
                target=self._worker,
                name="account_truth_snapshotter",
                daemon=True,
            )
            self._thread.start()

    def _init_schema(self) -> None:
        con = sqlite3.connect(self._db_path)
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute(
                """CREATE TABLE IF NOT EXISTS account_truth_snapshot (
                       account_id TEXT NOT NULL,
                       venue TEXT NOT NULL,
                       snapshot_ts REAL NOT NULL,
                       source TEXT,
                       positions TEXT NOT NULL,
                       cash_by_ccy TEXT NOT NULL,
                       PRIMARY KEY (account_id, venue)
                   )"""
            )
            con.commit()
        finally:
            con.close()

    def enqueue(self, snapshot: dict) -> None:
        if self._closed or snapshot is None:
            return
        try:
            payload = copy.deepcopy(snapshot)
        except Exception:
            return
        self._drained.clear()
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(payload)
            except queue.Full:
                pass

    def _worker(self) -> None:
        self.writer_thread_ident = threading.get_ident()
        con = sqlite3.connect(self._db_path)
        con.execute("PRAGMA journal_mode=WAL")
        try:
            while True:
                item = self._queue.get()
                if item is _SENTINEL:
                    self._queue.task_done()
                    break
                try:
                    self._write(con, item)
                except Exception:
                    pass
                finally:
                    self._queue.task_done()
                    if self._queue.empty():
                        self._drained.set()
        finally:
            con.close()

    def _write(self, con, snap: dict) -> None:
        con.execute(
            """INSERT INTO account_truth_snapshot
                   (account_id, venue, snapshot_ts, source, positions, cash_by_ccy)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(account_id, venue) DO UPDATE SET
                   snapshot_ts = excluded.snapshot_ts,
                   source = excluded.source,
                   positions = excluded.positions,
                   cash_by_ccy = excluded.cash_by_ccy
               WHERE excluded.snapshot_ts > account_truth_snapshot.snapshot_ts""",
            (
                snap["account_id"],
                snap["venue"],
                snap["snapshot_ts"],
                snap.get("source"),
                json.dumps(snap["positions"], default=_json_default),
                json.dumps(snap["cash_by_ccy"], default=_json_default),
            ),
        )
        con.commit()

    def drain_queue_for_test(self):
        out = []
        while True:
            try:
                out.append(self._queue.get_nowait())
            except queue.Empty:
                return out

    def flush(self, timeout: float = 2.0) -> bool:
        return self._drained.wait(timeout)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._thread is not None:
            try:
                self._queue.put_nowait(_SENTINEL)
            except queue.Full:
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                except queue.Empty:
                    pass
                try:
                    self._queue.put_nowait(_SENTINEL)
                except queue.Full:
                    pass
            self._thread.join(timeout=2.0)
