"""
pytest conftest for kis_futures connector tests.

Provides a module-scoped event loop fixture so that each test module gets a
fresh event loop.  This prevents "no current event loop" errors that occur
when asyncio.new_event_loop() is used inside sync test functions and the
previous loop was closed — the OrderBookTracker __init__ calls
asyncio.get_event_loop() which fails after the loop is closed.

The fixture sets the loop as the running default so all calls to
asyncio.get_event_loop() within the same thread succeed.
"""
import asyncio

import pytest


@pytest.fixture(autouse=True)
def set_event_loop():
    """Create a fresh event loop for each test and tear it down after."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)
