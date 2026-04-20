import asyncio
import logging
import queue
from typing import Any, Callable, Dict, Optional, Protocol

import orjson
from aiokafka import AIOKafkaProducer

from dynamic_des.connectors.egress.base import BaseEgress

logger = logging.getLogger(__name__)

# ==========================================
# Pluggable Serializer Interfaces
# ==========================================


class MessageSerializer(Protocol):
    """
    Protocol defining the interface for all Kafka egress serializers.

    Any custom serializer passed to `KafkaEgress` must implement the
    `serialize` method according to this signature.
    """

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given payload into a byte string for Kafka transmission.

        Args:
            topic (str): The destination Kafka topic. This is often required by
                schema registries (like Avro) to resolve the correct schema subject,
                even if simpler serializers (like JSON) choose to ignore it.
            data (Any): The payload to serialize. This is typically a raw dictionary
                or a Pydantic model instance.

        Returns:
            bytes: The fully encoded byte string ready to be published to Kafka.
        """
        ...


def _extract_dict(data: Any) -> dict:
    """
    Helper to seamlessly extract dicts from Pydantic V1/V2 objects.

    Args:
        data (Any): A raw dictionary or a Pydantic model.

    Returns:
        dict: The extracted dictionary representation of the data.
    """
    if isinstance(data, dict):
        return data
    if hasattr(data, "model_dump"):
        return data.model_dump(mode="json")
    if hasattr(data, "dict"):
        return data.dict()
    return data


class JsonSerializer:
    """
    Default fallback serializer providing backward compatibility via orjson.

    This serializer converts dictionaries or Pydantic models into standard
    JSON byte strings.
    """

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data to a JSON byte string.

        Args:
            topic (str): The target Kafka topic (unused by this serializer but required by protocol).
            data (Any): The payload to serialize (dictionary or Pydantic model).

        Returns:
            bytes: The JSON-encoded byte string.
        """
        payload = _extract_dict(data)
        return orjson.dumps(payload)


class ConfluentAvroSerializer:
    """
    Lazy-loaded serializer for Confluent Schema Registry using confluent-kafka.

    This class converts simulation data into Avro-encoded byte strings,
    automatically fetching and validating against the schema from a
    Confluent-compatible Schema Registry.
    """

    def __init__(self, registry_url: str, schema_str: str, **registry_kwargs: Any):
        """
        Initializes the Confluent Avro serializer.

        Args:
            registry_url (str): The base URL of the Confluent Schema Registry.
            schema_str (str): The Avro schema defined as a JSON string (typically
                generated directly from a Pydantic model via `.avro_schema()`).
            **registry_kwargs: Additional configuration arguments passed directly
                to the `SchemaRegistryClient` (e.g., `{'basic.auth.user.info': 'user:pass'}`).

        Raises:
            ImportError: If the 'confluent-kafka' package is not installed.
        """
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroSerializer
            from confluent_kafka.serialization import MessageField, SerializationContext
        except ImportError:
            raise ImportError(
                "The 'confluent-kafka' package is required to use ConfluentAvroSerializer. "
                "Install it using: pip install dynamic-des[confluent]"
            )

        # Merge the mandatory URL with any additional args (like basic.auth.user.info)
        conf = {"url": registry_url, **registry_kwargs}
        client = SchemaRegistryClient(conf)

        self.avro_serializer = AvroSerializer(client, schema_str)
        self.SerializationContext = SerializationContext
        self.MessageField = MessageField

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data into an Avro byte string.

        Args:
            topic (str): The target Kafka topic, used for schema subject naming.
            data (Any): The payload to serialize (dictionary or Pydantic model).

        Returns:
            bytes: The Avro-encoded byte string.
        """
        payload = _extract_dict(data)
        ctx = self.SerializationContext(topic, self.MessageField.VALUE)
        return self.avro_serializer(payload, ctx)


class GlueAvroSerializer:
    """
    Lazy-loaded serializer for AWS Glue Schema Registry using boto3.

    This class converts simulation data into Avro-encoded byte strings,
    integrating directly with AWS Glue Schema Registry for schema validation.
    """

    def __init__(self, registry_name: str, schema_str: str, **boto3_kwargs: Any):
        """
        Initializes the AWS Glue Avro serializer.

        Args:
            registry_name (str): The name of the AWS Glue Schema Registry.
            schema_str (str): The Avro schema defined as a JSON string (typically
                generated directly from a Pydantic model via `.avro_schema()`).
            **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
                initialization (e.g., `region_name`, `aws_access_key_id`).

        Raises:
            ImportError: If the 'aws-glue-schema-registry' or 'boto3' packages are not installed.
        """
        try:
            import boto3
            from aws_schema_registry import SchemaRegistryClient
            from aws_schema_registry.adapter.kafka import KafkaSerializer
            from aws_schema_registry.avro import AvroSchema
        except ImportError:
            raise ImportError(
                "The 'aws-glue-schema-registry' and 'boto3' packages are required. "
                "Install them using: pip install dynamic-des[glue]"
            )

        # Provide a default region if the user didn't explicitly pass one
        if "region_name" not in boto3_kwargs:
            boto3_kwargs["region_name"] = "us-east-1"

        # Pass the kwargs directly into boto3
        glue_client = boto3.client("glue", **boto3_kwargs)

        registry_client = SchemaRegistryClient(glue_client, registry_name=registry_name)
        schema = AvroSchema(schema_str)
        self.serializer = KafkaSerializer(registry_client, schema)

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data into an Avro byte string using AWS Glue.

        Args:
            topic (str): The target Kafka topic.
            data (Any): The payload to serialize (dictionary or Pydantic model).

        Returns:
            bytes: The Avro-encoded byte string.
        """
        payload = _extract_dict(data)
        return self.serializer.serialize(topic, payload)


# ==========================================
# Core Egress Class
# ==========================================


class KafkaEgress(BaseEgress):
    """
    High-throughput Kafka producer for simulation telemetry and events.

    This connector utilizes `aiokafka` for asynchronous I/O and `orjson` for fast
    serialization. It implements a resilient connection loop with exponential
    backoff.

    By default, data is routed to the `telemetry_topic` or `event_topic` based on
    its `stream_type`. If a `topic_router` callable is provided, topic selection
    is delegated to that function instead, allowing for advanced multiplexing
    (e.g., splitting ML vs. UI events).

    Attributes:
        bootstrap_servers (str): Comma-separated list of Kafka brokers.
        telemetry_topic (str): The default topic name for telemetry data.
        event_topic (str): The default topic name for lifecycle events.
        topic_router (Callable): Optional external logic to determine the topic.
        topic_serializers (Optional[Dict[str, MessageSerializer]]): Optional mapping of topics to specific serializers.
        default_serializer (Optional[MessageSerializer]): Fallback serializer if a topic is not in `topic_serializers`.
        producer_config (dict): Configuration dictionary passed to AIOKafkaProducer.

    Examples:
        Defining a custom topic router to split machine learning events from standard telemetry:

        ```python
        def custom_topic_router(data: dict) -> str:
            stream_type = data.get("stream_type")
            if stream_type == "telemetry":
                return "sim-telemetry"

            value = data.get("value", {})
            if isinstance(value, dict):
                event_type = value.get("event_type")
                if event_type == "prediction_request":
                    return "mill-predictions"
                elif event_type == "ground_truth":
                    return "mill-groundtruth"

            return "mill-lifecycle"

        # Pass it to the egress connector
        egress = KafkaEgress(
            bootstrap_servers="localhost:9092",
            topic_router=custom_topic_router
        )
        ```
    """

    def __init__(
        self,
        bootstrap_servers: str,
        telemetry_topic: str = "sim-telemetry",
        event_topic: str = "sim-events",
        topic_router: Optional[Callable[[dict], str]] = None,
        topic_serializers: Optional[Dict[str, MessageSerializer]] = None,
        default_serializer: Optional[MessageSerializer] = None,
        **kwargs: Any,
    ):
        """
        Initializes the KafkaEgress with topic and connection settings.

        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers.
            telemetry_topic: Destination topic for telemetry stream.
            event_topic: Destination topic for event stream.
            topic_router: Optional callable to dynamically route payloads to specific topics.
            topic_serializers: Mapping of target topics to their specific `MessageSerializer` implementations.
            default_serializer: The fallback serializer to use if a topic lacks a specific mapping. Defaults to `JsonSerializer`.
            **kwargs: Additional overrides for the AIOKafkaProducer configuration.
        """
        self.telemetry_topic = telemetry_topic
        self.event_topic = event_topic
        self.topic_router = topic_router

        # Initialize serializers in a backward-compatible way
        self.topic_serializers = topic_serializers or {}
        self.default_serializer = default_serializer or JsonSerializer()

        # High-performance defaults for 100k/sec
        self.producer_config = {
            "bootstrap_servers": bootstrap_servers,
            "linger_ms": 10,  # Batch messages for 10ms before sending
            "compression_type": "lz4",  # Fast compression for high volume
            "max_batch_size": 131072,  # 128KB batch size
            **kwargs,
        }

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        The main execution loop that consumes the egress queue and publishes to Kafka.

        This method maintains a persistent connection to Kafka. If the connection
        is lost, it implements an exponential backoff retry strategy. It polls
        the internal `egress_queue` for batches of data, determines the target
        topic based on the 'stream_type', and performs asynchronous sends using
        the configured serializers.

        Args:
            egress_queue: A thread-safe queue containing lists of dictionaries
                or Pydantic models generated by the EgressMixIn.

        Note:
            The loop exits gracefully upon receiving an `asyncio.CancelledError`,
            ensuring the Kafka producer is stopped correctly.
        """
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
                                if self.topic_router:
                                    # Use externalized logic if provided
                                    topic = self.topic_router(data)
                                    stream = data.pop("stream_type", "event")
                                else:
                                    # Fallback to standard dynamic-des routing
                                    stream = data.pop("stream_type")
                                    topic = (
                                        self.telemetry_topic
                                        if stream == "telemetry"
                                        else self.event_topic
                                    )

                                # Extract the Kafka Key
                                key = None
                                if stream == "telemetry":
                                    key = (
                                        str(data["path_id"]).encode()
                                        if "path_id" in data
                                        else None
                                    )
                                else:
                                    key = (
                                        str(data["key"]).encode()
                                        if "key" in data
                                        else None
                                    )

                                # Apply configured Serializer strategy
                                serializer = self.topic_serializers.get(
                                    topic, self.default_serializer
                                )
                                payload_bytes = serializer.serialize(topic, data)

                                await producer.send(topic, value=payload_bytes, key=key)

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
