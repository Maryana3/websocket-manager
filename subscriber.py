import asyncio
import dataclasses
import typing


class Subscriber:
    def __init__(self, *args, **kwargs):
        self.__subscribed = False
        self.__message_stack: set[Message] = set()
        self.__evpending = asyncio.Event()

    async def subscribe(self, *args, **kwargs):
        self.__subscribed = True

    async def unsubscribe(self, *args, **kwargs):
        self.__subscribed = False

    async def get_message(self, **kwargs):
        message = self.__message_stack.pop()
        self.__evpending.clear()
        return message

    async def publish(self, channel, message):
        message = Message(channel=bytes(channel, "utf-8"), data=bytes(message, "utf-8"))
        self.__message_stack.add(message)
        self.__evpending.set()

    async def listen(self) -> typing.AsyncIterator:
        while self.__subscribed:
            await self.__evpending.wait()
            response = await self.get_message()
            if response is not None:
                yield response.dict()


@dataclasses.dataclass(eq=False)
class Message:
    channel: bytes
    data: bytes

    dict = dataclasses.asdict
