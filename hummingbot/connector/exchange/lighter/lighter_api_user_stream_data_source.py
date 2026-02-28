import asyncio
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.lighter import lighter_constants as CONSTANTS
from hummingbot.connector.exchange.lighter.lighter_auth import LighterAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.lighter.lighter_exchange import LighterExchange


class LighterAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: LighterAuth,
        trading_pairs: List[str],
        connector: 'LighterExchange',
        api_factory: WebAssistantsFactory,
    ):
        super().__init__()
        self._auth: LighterAuth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange.
        Lighter uses the same WS endpoint for public and private streams;
        private channels are authenticated via account_index in each subscription message.
        """
        ws: WSAssistant = await self._get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_PRIVATE_URL)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            for channel in [CONSTANTS.PRIVATE_ORDER_CHANNEL_NAME, CONSTANTS.PRIVATE_TRADE_CHANNEL_NAME]:
                payload = {
                    "type": "subscribe",
                    "channel": channel,
                    "account_index": self._auth.account_index,
                }
                subscribe_request: WSJSONRequest = WSJSONRequest(payload=payload)

                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                    await websocket_assistant.send(subscribe_request)

            self.logger().info("Subscribed to private account orders and trades channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to private user stream channels...")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        async for ws_response in websocket_assistant.iter_messages():
            data: Dict[str, Any] = ws_response.data
            try:
                if isinstance(data, str):
                    json_data = json.loads(data)
                else:
                    json_data = data
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().warning(f"Invalid event message received through the user stream connection ({data})")
                continue

            if "error" in json_data:
                raise ValueError(f"Error message received in the user stream: {json_data}")

            await self._process_event_message(event_message=json_data, queue=queue)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if (
            len(event_message) > 0
            and "type" in event_message
            and event_message["type"].startswith("update/account_")
        ):
            queue.put_nowait(event_message)

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
