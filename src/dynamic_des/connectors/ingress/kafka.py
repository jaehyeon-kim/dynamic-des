import json
import queue

from aiokafka import AIOKafkaConsumer

from dynamic_des.connectors.ingress.base import BaseIngress


class KafkaIngress(BaseIngress):
    """
    Consumes JSON messages from Kafka and routes them to the Registry.
    Expected format: {"path_id": "Line_A.arrival.standard.rate", "value": 0.05}
    """

    def __init__(self, topic: str, bootstrap_servers: str, **kwargs):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.kwargs = kwargs

    async def run(self, ingress_queue: queue.Queue) -> None:
        consumer = AIOKafkaConsumer(
            self.topic, bootstrap_servers=self.bootstrap_servers, **self.kwargs
        )
        await consumer.start()
        try:
            async for msg in consumer:
                try:
                    data = json.loads(msg.value.decode("utf-8"))
                    # Use 'path_id' to match registry paths
                    ingress_queue.put((data["path_id"], data["value"]))
                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    print(f"KafkaIngress Error: {e}")
        finally:
            await consumer.stop()
