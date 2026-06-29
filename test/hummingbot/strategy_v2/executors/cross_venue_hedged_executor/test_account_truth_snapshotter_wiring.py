from types import SimpleNamespace

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)


class _FakeSnapshotter:
    def __init__(self):
        self.enqueued = []
        self.enqueued_count = 0
        self.closed = False

    def enqueue(self, snapshot):
        self.enqueued.append(snapshot)
        self.enqueued_count += 1

    def close(self):
        self.closed = True


class _Harness(CrossVenueHedgedExecutorBase):
    def __init__(self, enabled: bool):
        self.config = SimpleNamespace(
            account_truth_snapshot_enabled=enabled,
            account_truth_snapshot_interval_s=60.0,
        )
        self.hedge_connector = "kis"
        self.connectors = {"kis": SimpleNamespace()}
        self._last_truth_snapshot_ts = 0.0
        self._latency_recorder = None
        self._account_truth_snapshotter = _FakeSnapshotter() if enabled else None

    def _gates_open(self):
        return True

    def _compute_targets(self):
        return []

    def _should_reprice(self, targets):
        return False

    def _place_targets(self, targets):
        return None

    def _size_hedge(self, pending_base):
        return None

    def _maker_balance_candidate(self):
        return None


def test_disabled_flag_creates_no_snapshotter():
    h = _Harness(enabled=False)
    assert h._account_truth_snapshotter is None
    h._maybe_snapshot_account_truth(now=1000.0)


def test_enabled_flag_enqueues_hedge_truth():
    h = _Harness(enabled=True)
    hedge = h.connectors[h.hedge_connector]
    hedge.get_account_truth = lambda: {
        "account_id": "A",
        "venue": "kis",
        "snapshot_ts": 5.0,
        "source": "t",
        "positions": {},
        "cash_by_ccy": {"KRW": 0},
    }
    h._maybe_snapshot_account_truth(now=1000.0)
    assert h._account_truth_snapshotter.enqueued_count == 1
    assert h._account_truth_snapshotter.enqueued[0]["venue"] == "kis"


def test_snapshot_cadence_is_throttled():
    h = _Harness(enabled=True)
    h.connectors[h.hedge_connector].get_account_truth = lambda: {
        "account_id": "A",
        "venue": "kis",
        "snapshot_ts": 5.0,
        "source": "t",
        "positions": {},
        "cash_by_ccy": {"KRW": 0},
    }
    h._maybe_snapshot_account_truth(now=1000.0)
    h._maybe_snapshot_account_truth(now=1010.0)
    assert h._account_truth_snapshotter.enqueued_count == 1


def test_snapshot_errors_are_behavior_neutral():
    h = _Harness(enabled=True)
    h.connectors[h.hedge_connector].get_account_truth = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    h._maybe_snapshot_account_truth(now=1000.0)
    assert h._account_truth_snapshotter.enqueued_count == 0


def test_on_stop_closes_snapshotter():
    h = _Harness(enabled=True)
    h.on_stop()
    assert h._account_truth_snapshotter.closed is True
