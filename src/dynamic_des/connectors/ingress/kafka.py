import asyncio
import json
import logging
import queue
from typing import Any, Dict, Optional, Protocol

from aiokafka import AIOKafkaConsumer

from dynamic_des.connectors.ingress.base import BaseIngress

logger = logging.getLogger(__name__)

# ==========================================
# Pluggable Deserializer Interfaces
# ==========================================


class MessageDeserializer(Protocol):
    """
    Protocol defining the interface for all Kafka ingress deserializers.

    Any custom deserializer passed to `KafkaIngress` must implement the
    `deserialize` method according to this signature.
    """

    def deserialize(self, topic: str, payload: bytes) -> Any:
        """
        Deserializes a raw byte payload from Kafka into a Python object.

        Args:
            topic (str): The Kafka topic from which the message was consumed.
            payload (bytes): The raw binary payload of the Kafka message.

        Returns:
            Any: The deserialized data, typically a dictionary containing a 'path_id' and 'value'.
        """
        ...


class JsonDeserializer:
    """
    Default fallback deserializer providing backward compatibility via standard JSON.

    This deserializer assumes the incoming payload is a UTF-8 encoded JSON string.
    """

    def deserialize(self, topic: str, payload: bytes) -> Any:
        """
        Deserializes a JSON byte string.

        Args:
            topic (str): The Kafka topic from which the message was consumed (unused).
            payload (bytes): The UTF-8 encoded JSON byte string.

        Returns:
            Any: The parsed JSON data as a Python dictionary.
        """
        return json.loads(payload.decode("utf-8"))


class ConfluentAvroDeserializer:
    """
    Lazy-loaded deserializer for Confluent Schema Registry.

    This class converts Avro-encoded byte strings back into Python dictionaries.
    It automatically fetches the appropriate schema from the registry using the
    schema ID embedded within the message's binary payload, so no `schema_str`
    is required during initialization.
    """

    def __init__(self, registry_url: str, **registry_kwargs: Any):
        """
        Initializes the Confluent Avro deserializer.

        Args:
            registry_url (str): The base URL of the Confluent Schema Registry.
            **registry_kwargs: Additional configuration arguments passed directly
                to the `SchemaRegistryClient` (e.g., `{'basic.auth.user.info': 'user:pass'}`).

        Raises:
            ImportError: If the 'confluent-kafka' package is not installed.
        """
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroDeserializer
            from confluent_kafka.serialization import MessageField, SerializationContext
        except ImportError:
            raise ImportError(
                "The 'confluent-kafka' package is required to use ConfluentAvroDeserializer. "
                "Install it using: pip install dynamic-des[confluent]"
            )

        conf = {"url": registry_url, **registry_kwargs}
        client = SchemaRegistryClient(conf)

        # No schema string required! It reads the Schema ID from the message bytes.
        self.avro_deserializer = AvroDeserializer(client)
        self.SerializationContext = SerializationContext
        self.MessageField = MessageField

    def deserialize(self, topic: str, payload: bytes) -> Any:
        """
        Deserializes an Avro-encoded byte string.

        Args:
            topic (str): The Kafka topic from which the message was consumed.
            payload (bytes): The Avro-encoded binary payload containing the schema ID.

        Returns:
            Any: The deserialized data as a Python dictionary.
        """
        ctx = self.SerializationContext(topic, self.MessageField.VALUE)
        return self.avro_deserializer(payload, ctx)


class GlueAvroDeserializer:
    """
    Lazy-loaded deserializer for AWS Glue Schema Registry.

    This class converts Avro-encoded byte strings back into Python dictionaries,
    integrating directly with AWS Glue Schema Registry. It resolves schemas dynamically
    based on the metadata embedded in the AWS Glue message payload.
    """

    def __init__(self, registry_name: str, **boto3_kwargs: Any):
        """
        Initializes the AWS Glue Avro deserializer.

        Args:
            registry_name (str): The name of the AWS Glue Schema Registry.
            **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
                initialization (e.g., `region_name`, `aws_access_key_id`).

        Raises:
            ImportError: If the 'aws-glue-schema-registry' or 'boto3' packages are not installed.
        """
        try:
            import boto3
            from aws_schema_registry import SchemaRegistryClient
            from aws_schema_registry.adapter.kafka import KafkaDeserializer
        except ImportError:
            raise ImportError(
                "The 'aws-glue-schema-registry' and 'boto3' packages are required. "
                "Install them using: pip install dynamic-des[glue]"
            )

        if "region_name" not in boto3_kwargs:
            boto3_kwargs["region_name"] = "us-east-1"

        glue_client = boto3.client("glue", **boto3_kwargs)
        registry_client = SchemaRegistryClient(glue_client, registry_name=registry_name)

        self.deserializer = KafkaDeserializer(registry_client)

    def deserialize(self, topic: str, payload: bytes) -> Any:
        """
        Deserializes an Avro-encoded byte string using AWS Glue.

        Args:
            topic (str): The Kafka topic from which the message was consumed.
            payload (bytes): The Avro-encoded binary payload.

        Returns:
            Any: The raw data dictionary extracted from the AWS DataAndSchema object.
        """
        # AWS returns a DataAndSchema object, we just want the raw data dictionary
        result = self.deserializer.deserialize(topic, payload)
        return result.data


# ==========================================
# Core Ingress Class
# ==========================================


class KafkaIngress(BaseIngress):
    """
    Resilient Kafka consumer for dynamic simulation configuration updates.

    This connector subscribes to a specified Kafka topic and decodes incoming
    messages into state updates for the SimulationRegistry. It supports pluggable
    deserialization (Avro/JSON) while remaining 100% backward compatible.

    The expected resulting dictionary format must contain a 'path_id' string
    and a 'value' of any serializable type.

    Attributes:
        topic (str): The Kafka topic name used for configuration signals.
        bootstrap_servers (str): Comma-separated string of Kafka broker addresses.
        topic_deserializers (Optional[Dict[str, MessageDeserializer]]): Mapping of topics to specific deserializers.
        default_deserializer (Optional[MessageDeserializer]): Fallback deserializer. Defaults to `JsonDeserializer`.
        kwargs (dict): Additional configuration parameters for the AIOKafkaConsumer.
    """

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        topic_deserializers: Optional[Dict[str, MessageDeserializer]] = None,
        default_deserializer: Optional[MessageDeserializer] = None,
        **kwargs: Any,
    ):
        """
        Initializes the KafkaIngress with connection and subscription details.

        Args:
            topic: The Kafka topic to subscribe to.
            bootstrap_servers: Kafka broker address or list of addresses.
            topic_deserializers: Mapping of topics to their specific deserializers.
            default_deserializer: Fallback deserializer if the topic isn't explicitly mapped.
            **kwargs: Arbitrary keyword arguments passed to the underlying AIOKafkaConsumer.
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

        self.topic_deserializers = topic_deserializers or {}
        self.default_deserializer = default_deserializer or JsonDeserializer()

        self.kwargs = kwargs

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Main execution loop that consumes Kafka messages, deserializes them,
        and populates the ingress queue.
        """
        backoff = 1.0
        while True:
            try:
                consumer = AIOKafkaConsumer(
                    self.topic, bootstrap_servers=self.bootstrap_servers, **self.kwargs
                )
                await consumer.start()
                logger.info(f"Connected to Kafka Ingress topic: {self.topic}")
                backoff = 1.0

                try:
                    async for msg in consumer:
                        try:
                            # Apply configured Deserializer strategy
                            deserializer = self.topic_deserializers.get(
                                msg.topic, self.default_deserializer
                            )

                            # Decode binary payload -> Python Dict
                            data = deserializer.deserialize(msg.topic, msg.value)

                            ingress_queue.put((data["path_id"], data["value"]))

                        except Exception as e:
                            logger.warning(
                                f"Malformed or undecodable Kafka message: {e}"
                            )
                finally:
                    await consumer.stop()

            except asyncio.CancelledError:
                logger.info("Kafka Ingress shut down requested. Exiting loop.")
                break
            except Exception as e:
                logger.error(f"Kafka Ingress error: {e}. Retrying in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
