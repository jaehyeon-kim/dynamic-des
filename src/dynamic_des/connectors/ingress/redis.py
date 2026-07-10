import asyncio
import logging
import queue
from typing import Any

import orjson
import redis.asyncio as redis

from dynamic_des.connectors.ingress.base import BaseIngress

logger = logging.getLogger(__name__)


class RedisIngress(BaseIngress):
    """
    Asynchronous ingress provider for dynamic parameter updates via Redis Pub/Sub.

    This connector utilizes `redis.asyncio` to listen on a specified Redis channel
    for parameter updates, parsing them from JSON, and pushing them to the simulation's
    internal update queue.

    Attributes:
        url (str): The connection URL for the Redis server.
        channel_name (str): The Redis Pub/Sub channel to subscribe to.
        poll_interval (float): Sleep interval when no messages are found.
        client (redis.Redis | None): The underlying redis-py asynchronous client instance.
        pubsub (redis.client.PubSub | None): The active Pub/Sub subscription instance.
        **kwargs: Additional configuration dictionary passed to `redis.from_url`.
    """

    def __init__(
        self,
        url: str,
        channel_name: str = "params",
        poll_interval: float = 0.5,
        **kwargs: Any,
    ):
        """
        Initializes the RedisIngress with connection and channel settings.

        Args:
            url: Redis connection URL (e.g., redis://localhost:6379/0).
            channel_name: Redis Pub/Sub channel to subscribe to.
            poll_interval: Sleep interval when no messages are found.
            **kwargs: Additional configuration dictionary passed to `redis.from_url`.
        """
        self.url = url
        self.channel_name = channel_name
        self.poll_interval = poll_interval
        self.kwargs = kwargs
        self.client: redis.Redis | None = None
        self.pubsub: redis.client.PubSub | None = None

    async def run(self, update_queue: queue.Queue) -> None:
        """
        The main execution loop for subscribing to Redis and receiving updates.

        Continuously polls the Pub/Sub channel for incoming JSON messages containing
        `param_path` and `param_value`, placing successfully parsed updates onto the
        internal update queue.

        Args:
            update_queue: A thread-safe queue where incoming parameters are pushed
                so they can be applied within the SimPy context.
        """
        self.client = redis.from_url(self.url, **self.kwargs)
        self.pubsub = self.client.pubsub()
        await self.pubsub.subscribe(self.channel_name)
        logger.info(f"RedisIngress subscribed to channel '{self.channel_name}'")

        try:
            while True:
                message = await self.pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=self.poll_interval
                )
                if message and message["type"] == "message":
                    try:
                        data = orjson.loads(message["data"])
                        param_path = data.get("param_path")
                        param_value = data.get("param_value")
                        if param_path is not None and param_value is not None:
                            update_queue.put((param_path, param_value))
                    except Exception as e:
                        logger.error(f"Failed to parse Redis Ingress message: {e}")
                else:
                    await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            pass
        finally:
            if self.pubsub:
                await self.pubsub.unsubscribe(self.channel_name)
            if self.client:
                await self.client.close()
