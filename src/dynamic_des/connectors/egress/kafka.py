import asyncio
import logging
import queue

import orjson
from aiokafka import AIOKafkaProducer

from dynamic_des.connectors.egress.base import BaseEgress

logger = logging.getLogger(__name__)


class KafkaEgress(BaseEgress):
    """
    High-throughput Kafka producer using orjson and batching.
    Routes 'telemetry' to a single-partition topic and 'event' to partitioned topics.
    """

    def __init__(
        self, telemetry_topic: str, event_topic: str, bootstrap_servers: str, **kwargs
    ):
        self.telemetry_topic = telemetry_topic
        self.event_topic = event_topic

        # High-performance defaults for 100k/sec
        self.producer_config = {
            "bootstrap_servers": bootstrap_servers,
            "linger_ms": 10,  # Batch messages for 10ms before sending
            "compression_type": "lz4",  # Fast compression for high volume
            "max_batch_size": 131072,  # 128KB batch size
            **kwargs,
        }

    async def run(self, egress_queue: queue.Queue) -> None:
        backoff = 1.0
        max_backoff = 60.0

        while True:
            try:
                producer = AIOKafkaProducer(**self.producer_config)
                await producer.start()
                logger.info("Kafka Egress producer connected successfully.")
                backoff = 1.0  # Reset backoff on successful connection

                try:
                    while True:
                        try:
                            # 'batch' is a list of dictionaries from the EgressMixIn
                            batch = egress_queue.get_nowait()

                            for data in batch:
                                stream = data.pop("stream_type")
                                if stream == "telemetry":
                                    topic = self.telemetry_topic
                                    key = (
                                        str(data["path_id"]).encode()
                                        if "key" in data
                                        else None
                                    )
                                else:
                                    topic = self.event_topic
                                    # Use the event_key for Kafka partitioning
                                    key = (
                                        str(data["key"]).encode()
                                        if "key" in data
                                        else None
                                    )

                                # orjson.dumps returns bytes directly (faster than json.dumps + encode)
                                await producer.send(
                                    topic, value=orjson.dumps(data), key=key
                                )

                        except queue.Empty:
                            # Yield to loop if queue is empty
                            await asyncio.sleep(0.001)
                finally:
                    await producer.stop()

            except asyncio.CancelledError:
                logger.info("Kafka Egress shut down requested. Exiting loop.")
                break
            except Exception as e:
                logger.error(
                    f"Kafka Egress connection failed: {e}. Retrying in {backoff} seconds..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
