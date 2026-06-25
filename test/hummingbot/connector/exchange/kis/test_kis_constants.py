from hummingbot.connector.exchange.kis import kis_constants as CONSTANTS


def test_balance_path_tightly_throttled_to_avoid_egw00215():
    # EGW00215 ("원장에서 허용 가능한 초당 거래건수를 초과"): KIS inquire-balance hits a
    # per-second account-LEDGER limit that is far below the connector's nominal REST budget.
    # BOTH _update_balances (periodic poll) AND _make_network_check_request (health probe)
    # call _api_get(path_url=DOMESTIC_STOCK_BALANCE_PATH) with NO explicit limit_id, so the
    # AsyncThrottler keys them on the PATH (ExchangePyBase: throttler_limit_id = limit_id or
    # path_url). At the old 10/s the bucket never blocked, so 2-3 coincident balance inquiries
    # (probe + poll + JEP-210 _refresh_hedge_balance) hit KIS in the same second -> HTTP 500
    # EGW00215. Serialize them to <=1/s so coincident calls spread across seconds. Also the
    # leading trigger hypothesis for the JEP-218 cold-boot throttle-storm freeze.
    balance_limits = [
        rl for rl in CONSTANTS.RATE_LIMITS if rl.limit_id == CONSTANTS.DOMESTIC_STOCK_BALANCE_PATH
    ]
    assert len(balance_limits) == 1, "exactly one RateLimit must own the balance path bucket"
    rl = balance_limits[0]
    assert rl.time_interval == 1
    assert rl.limit == 1, f"balance path must be serialized to 1/s to avoid EGW00215; got {rl.limit}/s"


def test_market_status_routing_ids():
    assert CONSTANTS.WS_MARKET_STATUS_TR_ID_BY_ROUTING[CONSTANTS.MARKET_ROUTING_KRX] == "H0STMKO0"
    assert CONSTANTS.WS_MARKET_STATUS_TR_ID_BY_ROUTING[CONSTANTS.MARKET_ROUTING_NXT] == "H0NXMKO0"
    assert CONSTANTS.WS_MARKET_STATUS_TR_ID_BY_ROUTING[CONSTANTS.MARKET_ROUTING_SOR] == "H0UNMKO0"


def test_market_status_columns_include_halt_gate_fields():
    assert len(CONSTANTS.WS_MARKET_STATUS_COLUMNS) == 11
    assert CONSTANTS.WS_MARKET_STATUS_COLUMNS[3] == "MKOP_CLS_CODE"
    assert "TRHT_YN" in CONSTANTS.WS_MARKET_STATUS_COLUMNS
    assert "VI_CLS_CODE" in CONSTANTS.WS_MARKET_STATUS_COLUMNS
    assert "OVTM_VI_CLS_CODE" in CONSTANTS.WS_MARKET_STATUS_COLUMNS


def test_market_status_decode_tables_are_fail_closed():
    assert "174" in CONSTANTS.MKOP_CLS_SET_CB
    assert "184" in CONSTANTS.MKOP_CLS_SET_CB
    assert "175" in CONSTANTS.MKOP_CLS_CLEAR_CB
    assert "185" in CONSTANTS.MKOP_CLS_CLEAR_CB
    assert "164" in CONSTANTS.MKOP_CLS_SET_TEMP_STOP

    assert CONSTANTS.MKOP_CLS_SET_CB.isdisjoint(CONSTANTS.MKOP_CLS_CLEAR_CB)
    sidecar_codes = {"387", "388", "397", "398"}
    assert sidecar_codes.isdisjoint(CONSTANTS.MKOP_CLS_SET_CB)
    assert sidecar_codes.isdisjoint(CONSTANTS.MKOP_CLS_CLEAR_CB)
    assert sidecar_codes.isdisjoint(CONSTANTS.MKOP_CLS_SET_TEMP_STOP)

    assert CONSTANTS.TRHT_HALTED == "Y"
    assert "Y" in CONSTANTS.VI_ACTIVE_VALUES
    assert CONSTANTS.KNOWN_NORMAL_MKOP == frozenset()
