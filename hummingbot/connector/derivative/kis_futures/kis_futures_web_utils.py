from typing import Callable, Optional

import hummingbot.connector.exchange.kis.kis_web_utils as spot_web
from hummingbot.connector.derivative.kis_futures import kis_futures_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Full URL for a public REST endpoint (delegates to spot web_utils)."""
    return spot_web.public_rest_url(path_url, domain)


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Full URL for a private REST endpoint (same base as public for KIS)."""
    return spot_web.private_rest_url(path_url, domain)


def ws_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """WebSocket URL; pass ``'sandbox'`` for the KIS sandbox environment."""
    return spot_web.ws_url(domain)


def reconnect_backoff(failures: int, base: float = 5.0, cap: float = 60.0) -> float:
    """Capped exponential backoff for WebSocket reconnect loops."""
    return spot_web.reconnect_backoff(failures, base, cap)


def create_throttler() -> AsyncThrottler:
    """AsyncThrottler configured with KIS futures rate limits."""
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """Builds a WebAssistantsFactory with throttler and optional auth."""
    return WebAssistantsFactory(
        throttler=throttler or create_throttler(),
        auth=auth,
    )
