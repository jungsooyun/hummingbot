import asyncio
import time
import unittest

from hummingbot.connector.exchange.kis import kis_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class KisAccountThrottleTests(unittest.IsolatedAsyncioTestCase):
    def _throttler(self) -> AsyncThrottler:
        # safety_margin_pct=0 -> deterministic boundary at the nominal limit.
        return AsyncThrottler(CONSTANTS.RATE_LIMITS, safety_margin_pct=0)

    async def _consume(self, throttler: AsyncThrottler, limit_id: str, n: int) -> None:
        for _ in range(n):
            async with throttler.execute_task(limit_id=limit_id):
                pass

    async def test_account_ceiling_boundary_allows_exactly_15(self):
        # arithmetic: ticker(per-path 20)로 account 15슬롯 경계 확인 — 15는 통과, 16번째는 대기.
        throttler = self._throttler()
        await self._consume(throttler, CONSTANTS.DOMESTIC_STOCK_TICKER_PATH, 14)
        ctx_15 = throttler.execute_task(limit_id=CONSTANTS.DOMESTIC_STOCK_TICKER_PATH)
        self.assertTrue(ctx_15.within_capacity(), "15th call must still fit under account=15")
        await self._consume(throttler, CONSTANTS.DOMESTIC_STOCK_TICKER_PATH, 1)  # now 15 consumed
        ctx_16 = throttler.execute_task(limit_id=CONSTANTS.DOMESTIC_STOCK_TICKER_PATH)
        self.assertFalse(ctx_16.within_capacity(), "16th call must wait")

    async def test_account_ceiling_aggregates_across_paths(self):
        # arithmetic: ticker로 15슬롯을 채우면 다른 path(order)도 account 포화로 진입 불가 — cross-path 합산.
        throttler = self._throttler()
        await self._consume(throttler, CONSTANTS.DOMESTIC_STOCK_TICKER_PATH, 15)
        ctx = throttler.execute_task(limit_id=CONSTANTS.DOMESTIC_STOCK_ORDER_PATH)
        self.assertFalse(ctx.within_capacity(), "account bucket must block a 16th call on a different path")

    async def test_account_ceiling_delays_concurrent_runtime(self):
        # 런타임 강제: per-path 미포화(ticker 8/20, order-detail 8/10)이지만 합 16 > account 15.
        # 동시 16콜을 gather하면 account 버킷이 16번째를 window(1s) 롤오버까지 지연시켜야 한다.
        throttler = self._throttler()
        ids = ([CONSTANTS.DOMESTIC_STOCK_TICKER_PATH] * 8) + ([CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH] * 8)

        async def one(limit_id: str) -> None:
            async with throttler.execute_task(limit_id=limit_id):
                pass

        start = time.monotonic()
        await asyncio.gather(*[one(limit_id) for limit_id in ids])
        elapsed = time.monotonic() - start
        # 16번째는 account가 ~1 window 대기시켜야 함(per-path는 둘 다 여유). retry_interval=0.1 고려 하한 0.8s.
        self.assertGreater(elapsed, 0.8, "account bucket must delay the 16th cross-path call ~1 window at runtime")

    async def test_per_path_independence_regression(self):
        # ORDER per-path 5/s는 account 도입 후에도 그대로 binding (account=15는 5보다 느슨).
        throttler = self._throttler()
        await self._consume(throttler, CONSTANTS.DOMESTIC_STOCK_ORDER_PATH, 5)
        ctx = throttler.execute_task(limit_id=CONSTANTS.DOMESTIC_STOCK_ORDER_PATH)
        self.assertFalse(ctx.within_capacity(), "ORDER per-path 5/s must still cap independently")

    def test_auth_paths_bypass_throttler(self):
        # token/WS approval은 직접 aiohttp(kis_auth.py)라 throttler를 거치지 않는다 — account 버킷 미계측.
        # 회귀로 누가 이들을 RATE_LIMITS에 linked하면 깨지도록 가드(현 설계: 의도적 throttler-blind).
        throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        for limit_id in (CONSTANTS.TOKEN_PATH_URL, CONSTANTS.WS_APPROVAL_PATH_URL):
            _, related = throttler.get_related_limits(limit_id)
            self.assertNotIn(
                CONSTANTS.KIS_ACCOUNT_REST_LIMIT_ID,
                [rl.limit_id for rl, _ in related],
                f"{limit_id} is throttler-blind by design; do not link account bucket",
            )
