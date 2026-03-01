import asyncio
import time

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class UpbitAPIUserStreamDataSource(UserStreamTrackerDataSource):
    def __init__(self):
        super().__init__()
        self._last_recv_time = 0.0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        # NOTE:
        # Upbit private websocket stream is not implemented yet in this connector.
        # Keep last_recv_time updated so connector readiness can rely on REST polling fallback.
        while True:
            self._last_recv_time = time.time()
            await asyncio.sleep(30)
