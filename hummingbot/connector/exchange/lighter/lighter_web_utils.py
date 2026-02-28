import time
from typing import Optional

import hummingbot.connector.exchange.lighter.lighter_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint.

    :param path_url: a public REST endpoint
    :param domain: not used for Lighter, kept for interface compatibility
    :return: the full URL to the endpoint
    """
    return f"{CONSTANTS.REST_URL}/{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint.

    :param path_url: a private REST endpoint
    :param domain: not used for Lighter, kept for interface compatibility
    :return: the full URL to the endpoint
    """
    return f"{CONSTANTS.REST_URL}/{path_url}"


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        auth: Optional[AuthBase] = None) -> WebAssistantsFactory:
    """
    Builds an API factory for Lighter REST API calls.

    :param throttler: optional throttler for rate limiting
    :param auth: optional auth for authenticated requests
    :return: a WebAssistantsFactory instance
    """
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
    )
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(
        throttler: AsyncThrottler) -> WebAssistantsFactory:
    """
    Builds an API factory without time synchronizer pre-processor.
    Lighter is a DEX and does not require time synchronization.

    :param throttler: throttler for rate limiting
    :return: a WebAssistantsFactory instance
    """
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


def create_throttler() -> AsyncThrottler:
    """
    Creates an AsyncThrottler with Lighter's rate limits.

    :return: an AsyncThrottler instance
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN) -> float:
    """
    Gets the current server time. Lighter is a DEX and does not provide
    a server time endpoint, so we return local time.

    :param throttler: optional throttler (unused)
    :param domain: optional domain (unused)
    :return: current time as float
    """
    return time.time()
