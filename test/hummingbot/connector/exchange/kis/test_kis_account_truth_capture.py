import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from hummingbot.connector.exchange.kis.kis_exchange import KisExchange


class TestKisAccountTruthCapture:
    BALANCE_RESPONSE = {
        "rt_cd": "0",
        "msg1": "정상",
        "output1": [
            {
                "pdno": "005930",
                "hldg_qty": "9",
                "ord_psbl_qty": "9",
                "pchs_avg_pric": "71500",
                "prpr": "72000",
            },
            {
                "pdno": "000660",
                "hldg_qty": "4",
                "ord_psbl_qty": "4",
                "pchs_avg_pric": "175000",
                "prpr": "176000",
            },
        ],
        "output2": [{"dnca_tot_amt": "1234567", "ord_psbl_cash": "1000000"}],
    }

    def setup_method(self):
        # KisExchange.__init__ -> OrderBookTracker.__init__ calls asyncio.get_event_loop(),
        # which raises under Python 3.12 when a prior test left no current loop on MainThread.
        # Establish a dedicated loop per test unconditionally so construction is robust in the
        # full-suite run (not just when this file runs alone).
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.exchange = KisExchange(
            kis_app_key="app-key",
            kis_app_secret="app-secret",
            kis_account_number="12345678-01",
            trading_pairs=["005930-KRW", "000660-KRW"],
            trading_required=False,
        )

    def teardown_method(self):
        self.loop.close()

    def test_capture_keeps_raw_truth_fields(self):
        with patch.object(self.exchange, "_api_get", new=AsyncMock(return_value=self.BALANCE_RESPONSE)):
            self.loop.run_until_complete(self.exchange._update_balances())

        truth = self.exchange.get_account_truth()

        assert truth["venue"] == "kis"
        assert truth["account_id"] == "12345678"
        assert truth["positions"]["005930"] == {
            "qty": Decimal("9"),
            "avg_entry": Decimal("71500"),
            "mark": Decimal("72000"),
            "ccy": "KRW",
        }
        assert truth["positions"]["000660"]["avg_entry"] == Decimal("175000")
        assert truth["cash_by_ccy"]["KRW"] == Decimal("1234567")
        assert truth["snapshot_ts"] > 0

    def test_capture_introduces_no_extra_kis_rest_call(self):
        api_get = AsyncMock(return_value=self.BALANCE_RESPONSE)
        with patch.object(self.exchange, "_api_get", api_get):
            self.loop.run_until_complete(self.exchange._update_balances())

        assert api_get.call_count == 1
