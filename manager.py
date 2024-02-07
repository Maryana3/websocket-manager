import asyncio
import logging

from fastapi import WebSocket

from app.models.meta import Singleton
from app.websocket.broker import BaseBroker
from app.websocket.utils import broker_from_url

__all__ = ("WebSocketManager",)


logger = logging.getLogger(__name__)


class WebSocketManager(metaclass=Singleton):
    """Base WebSocket manager."""

    def __init__(self, broker_url) -> None:
        self.sockets: dict[WebSocket] = dict()
        self.broker: BaseBroker = broker_from_url(broker_url)

        # Events
        self.__evsubscribed = asyncio.Event()

        # Channels
        self.__chmain = "channel:main"

    async def startup(self):
        await self.broker.connect()

        # subscribe to main broker channel
        # NOTE: this channel should simply be open so that we always have something to
        # listen to (see `redis.asyncio.client.PubSub.listen()` implementation for details)
        # but you can use this channel to send/receive some messages too.
        await self.broker.subscribe(self.__chmain)
        self.__evsubscribed.set()

        asyncio.create_task(  # create listening task
            self._get_message(
                self.broker.subscriber,
                # < events >
                # we don't want to end `listen()` coro so we need
                # at least one channel (subscriber) to listen to
                self.__evsubscribed,
            )
        )

    async def shutdown(self):
        await self.broker.unsubscribe(self.__chmain)
        self.__evsubscribed.clear()
        await self.broker.disconnect()

    async def connect(self, socket_id: str, websocket: WebSocket, **kwargs):
        """Connect and subscribe websocket to topic."""
        await websocket.accept(**kwargs)

        self.sockets[socket_id] = websocket
        await self.broker.subscribe(socket_id)

    async def disconnect(self, socket_id: str) -> None:
        """Unsubscribe websocket.

        NOTE: this method doesn't close websocket connection, further messages for
        this websocket will be ignored.
        """
        if socket_id not in self.sockets:
            raise ValueError(f"socket with id {socket_id} not found.")

        del self.sockets[socket_id]
        await self.broker.unsubscribe(socket_id)

    async def send_message(self, message, socket_id):
        """Publish message on channel `socket_id`."""
        await self.broker.publish(message=message, socket_id=socket_id)

    async def _get_message(self, subscriber, *evnts: list[asyncio.Event]):
        """Listen all channels and send messages.

        NOTE: this method is for internal use only and should not be accessed
        from the outside.
        """
        for event in evnts:
            # we expect all events to happen before
            # actually start to listen to channels
            await event.wait()

        async for message in subscriber.listen():
            if message is not None:
                socket_id = message["channel"].decode("utf-8")
                socket = self.sockets[socket_id]
                try:
                    data = message["data"].decode("utf-8")
                    await socket.send_text(data)
                except UnicodeDecodeError:
                    await socket.send_bytes(message["data"])
