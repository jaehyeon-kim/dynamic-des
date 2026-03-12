import asyncio
import json
import logging
import queue

from aiokafka import AIOKafkaConsumer

from dynamic_des.connectors.ingress.base import BaseIngress

logger = logging.getLogger(__name__)


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
        backoff = 1.0
        max_backoff = 60.0

        while True:
            try:
                consumer = AIOKafkaConsumer(
                    self.topic, bootstrap_servers=self.bootstrap_servers, **self.kwargs
                )
                await consumer.start()
                logger.info(f"Kafka Ingress connected to topic '{self.topic}'.")
                backoff = 1.0  # Reset backoff on successful connection

                try:
                    async for msg in consumer:
                        try:
                            data = json.loads(msg.value.decode("utf-8"))
                            # Use 'path_id' to match registry paths
                            ingress_queue.put((data["path_id"], data["value"]))
                        except (json.JSONDecodeError, KeyError, TypeError) as e:
                            # Bad payload shouldn't crash the consumer
                            logger.warning(
                                f"KafkaIngress Data Error: {e}. Payload: {msg.value}"
                            )
                finally:
                    await consumer.stop()

            except asyncio.CancelledError:
                logger.info("Kafka Ingress shut down requested. Exiting loop.")
                break
            except Exception as e:
                logger.error(
                    f"Kafka Ingress connection failed: {e}. Retrying in {backoff} seconds..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
