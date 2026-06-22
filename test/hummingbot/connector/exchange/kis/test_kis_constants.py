from hummingbot.connector.exchange.kis import kis_constants as CONSTANTS


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
