import queue

from dynamic_des.connectors.ingress.base import BaseIngress


class RedisIngress(BaseIngress):
    """
    Consumes updates from Redis Pub/Sub or Key-Value store.
    Planned for a future release.
    """

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("RedisIngress is planned for a future release.")

    async def run(self, ingress_queue: queue.Queue) -> None:
        pass
