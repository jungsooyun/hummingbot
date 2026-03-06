class BithumbSelfTradePreventionError(Exception):
    """Raised when Bithumb rejects an order due to self-trade prevention (cross_trading)."""

