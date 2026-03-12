import asyncio
import json
import logging
import queue
from typing import Any

from aiokafka import AIOKafkaConsumer

from dynamic_des.connectors.ingress.base import BaseIngress

logger = logging.getLogger(__name__)


class KafkaIngress(BaseIngress):
    """
    Resilient Kafka consumer for dynamic simulation configuration updates.

    This connector subscribes to a specified Kafka topic and parses incoming
    JSON messages into state updates for the SimulationRegistry. It is designed
    to handle real-time control signals that modify simulation parameters
    (e.g., changing a machine's capacity or an arrival rate) while the model
    is executing.

    The expected message format is a JSON object containing a 'path_id' string
    and a 'value' of any serializable type.

    Attributes:
        topic (str): The Kafka topic name used for configuration signals.
        bootstrap_servers (str): Comma-separated string of Kafka broker addresses.
        kwargs (dict): Additional configuration parameters for the AIOKafkaConsumer.
    """

    def __init__(self, topic: str, bootstrap_servers: str, **kwargs: Any):
        """
        Initializes the KafkaIngress with connection and subscription details.

        Args:
            topic: The Kafka topic to subscribe to.
            bootstrap_servers: Kafka broker address or list of addresses.
            **kwargs: Arbitrary keyword arguments passed to the underlying
                AIOKafkaConsumer (e.g., group_id, security_protocol).
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.kwargs = kwargs

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Main execution loop that consumes Kafka messages and populates the ingress queue.

        This method maintains a persistent connection to the Kafka broker. It
        features an exponential backoff strategy to handle network interruptions
        and connection failures gracefully.

        Decoded messages are validated for basic structure; malformed JSON or
        missing keys are logged as warnings without halting the consumer.

        Args:
            ingress_queue: A thread-safe queue where (path_id, value) tuples
                are placed for processing by the SimulationRegistry.

        Note:
            The loop responds to `asyncio.CancelledError` for clean shutdown,
            ensuring the Kafka consumer stops and releases resources properly.
        """
        backoff = 1.0
        while True:
            try:
                consumer = AIOKafkaConsumer(
                    self.topic, bootstrap_servers=self.bootstrap_servers, **self.kwargs
                )
                await consumer.start()
                logger.info(f"Connected to Kafka topic: {self.topic}")
                backoff = 1.0
                try:
                    async for msg in consumer:
                        try:
                            data = json.loads(msg.value.decode("utf-8"))
                            ingress_queue.put((data["path_id"], data["value"]))
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.warning(f"Malformed Kafka message: {e}")
                finally:
                    await consumer.stop()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Kafka Ingress error: {e}. Retrying in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
