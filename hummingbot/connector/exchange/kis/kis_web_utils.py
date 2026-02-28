from typing import Callable, Optional

import hummingbot.connector.exchange.kis.kis_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint.

    :param path_url: a public REST endpoint
    :param domain: pass ``"sandbox"`` to target the KIS sandbox environment

    :return: the full URL to the endpoint
    """
    base = CONSTANTS.REST_SANDBOX_URL if domain == "sandbox" else CONSTANTS.REST_URL
    return f"{base}/{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint.

    KIS uses the same base URL for public and private endpoints; authentication
    is handled via headers (``Authorization``, ``appkey``, ``appsecret``,
    ``tr_id``).

    :param path_url: a private REST endpoint
    :param domain: pass ``"sandbox"`` to target the KIS sandbox environment

    :return: the full URL to the endpoint
    """
    return public_rest_url(path_url, domain)


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """
    Builds an API factory with throttler and optional auth.

    :param throttler: the throttler instance to use for rate limiting
    :param time_synchronizer: unused for KIS (no server time sync required)
    :param time_provider: unused for KIS (no server time sync required)
    :param auth: the auth instance to attach to requests

    :return: a configured WebAssistantsFactory
    """
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
    )
    return api_factory


def create_throttler() -> AsyncThrottler:
    """Creates and returns an AsyncThrottler configured with KIS rate limits."""
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)
