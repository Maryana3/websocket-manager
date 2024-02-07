from urllib.parse import urlparse
from uuid import uuid4

from app.websocket.broker import BaseBroker, InMemoryBroker, RedisBroker


def broker_from_url(broker_url: str) -> BaseBroker:
    url = urlparse(broker_url)
    if url.scheme in ["redis", "rediss"]:
        return RedisBroker(url=broker_url)
    elif url.scheme == "memory":
        return InMemoryBroker(url=broker_url)
    raise ValueError(f"unknown broker url `{broker_url}`.")


def generate_socket_id() -> str:
    return str(uuid4())
