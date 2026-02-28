import asyncio

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class UpbitAPIUserStreamDataSource(UserStreamTrackerDataSource):
    async def listen_for_user_stream(self, output: asyncio.Queue):
        await asyncio.Event().wait()
