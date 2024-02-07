from abc import ABC, abstractmethod, abstractproperty

import redis.asyncio as aioredis

from app.websocket.subscriber import Subscriber


class BaseBroker(ABC):
    """Base WebSocket broker interface."""

    @abstractproperty
    def subscriber(self):
        ...

    @abstractmethod
    async def connect(self, *args, **kwargs):
        ...

    @abstractmethod
    async def disconnect(self, *args, **kwargs):
        ...

    @abstractmethod
    async def subscribe(self, *args, **kwargs):
        ...

    @abstractmethod
    async def unsubscribe(self, *args, **kwargs):
        ...

    @abstractmethod
    async def publish(self, *args, **kwargs):
        ...


class InMemoryBroker(BaseBroker):
    """In-memory broker.

    NOTE: DO NOT use in production, please
    """

    def __init__(self, url) -> None:
        del url
        self._subscriber = None

    @property
    def subscriber(self):
        return self._subscriber

    async def connect(self, *args, **kwargs):
        self._subscriber = Subscriber(*args, **kwargs)

    async def disconnect(self, *args, **kwargs):
        self._subscriber = None

    async def subscribe(self, *args, **kwargs):
        await self._subscriber.subscribe()
        return self._subscriber

    async def unsubscribe(self, *args, **kwargs):
        await self._subscriber.unsubscribe()
        return self._subscriber

    async def publish(self, socket_id, message):
        await self._subscriber.publish(socket_id, message)


class RedisBroker(BaseBroker):
    """Redis broker.

    Messages are published using Redis PUB/SUB service. To send a message, it is enough
    to know the channel. Websockets that are subscribed to this channel will automatically
    read this message and send it to the client (no matter in which instance or process it
    was connected).
    """

    def __init__(self, url) -> None:
        self.url = url
        self.pubsub = None
        self.redis_connection = None

    @property
    def default_connection_options(self):
        """Default options that will be included during broker connection."""
        return {}

    @property
    def default_pubsub_options(self):
        return {"ignore_subscribe_messages": True}

    @property
    def subscriber(self):
        return self.pubsub

    async def _establish_connection(self, *args, **kwargs):
        return aioredis.Redis.from_url(self.url, *args, **kwargs)

    async def connect(self, *args, **kwargs):
        """Connect to the Redis server."""
        kwargs.update(self.default_connection_options)
        self.redis_connection = await self._establish_connection(*args, **kwargs)
        self.pubsub = self.redis_connection.pubsub(**self.default_pubsub_options)

    async def disconnect(self, *args, **kwargs):
        """Disconnect from the Redis server."""
        if self.redis_connection:
            return await self.redis_connection.close(*args, **kwargs)

    async def publish(self, socket_id, message):
        """Publish message to the channel `socket_id`."""
        if self.redis_connection is None:
            raise RuntimeError("redis connection is not established.")

        await self.redis_connection.publish(socket_id, message)

    async def subscribe(self, socket_id):
        """Add channel to pubsub."""
        await self.pubsub.subscribe(socket_id)
        return self.pubsub

    async def unsubscribe(self, socket_id):
        """Remove channel from pubsub."""
        await self.pubsub.unsubscribe(socket_id)
