import asyncio
import json
import queue
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynamic_des.connectors.ingress.kafka import (
    ConfluentAvroDeserializer,
    GlueAvroDeserializer,
    KafkaIngress,
)


def test_missing_avro_deserializer_dependencies():
    """Verify that using Avro deserializers without the extras raises an explicit ImportError."""
    # Hide confluent_kafka
    with patch.dict(sys.modules, {"confluent_kafka": None}):
        with pytest.raises(ImportError, match=r"pip install dynamic-des\[confluent\]"):
            ConfluentAvroDeserializer(registry_url="http://mock")

    # Hide boto3
    with patch.dict(sys.modules, {"boto3": None}):
        with pytest.raises(ImportError, match=r"pip install dynamic-des\[glue\]"):
            GlueAvroDeserializer(registry_name="mock")


@pytest.mark.asyncio
@patch("dynamic_des.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_kafka_ingress_pluggable_deserializers(MockConsumer):
    """Verify that topic_deserializers correctly route binary payloads to custom deserializers."""

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            class FakeMessage:
                topic = "secure-topic"
                value = b"MOCK_BINARY_AVRO_PAYLOAD"

            yield FakeMessage()
            await asyncio.sleep(999)

    MockConsumer.return_value = FakeConsumer()

    # Create a mock deserializer that returns a specific dictionary
    mock_deserializer = MagicMock()
    mock_deserializer.deserialize.return_value = {
        "path_id": "Line_A.lathe.current_cap",
        "value": 10,
    }

    ingress_queue = queue.Queue()
    ingress = KafkaIngress(
        topic="secure-topic",
        bootstrap_servers="localhost:9092",
        topic_deserializers={"secure-topic": mock_deserializer},
    )

    task = asyncio.create_task(ingress.run(ingress_queue))
    await asyncio.sleep(0.1)

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify the deserializer intercepted the message
    mock_deserializer.deserialize.assert_called_once_with(
        "secure-topic", b"MOCK_BINARY_AVRO_PAYLOAD"
    )

    # Verify the decoded data was placed in the queue
    assert not ingress_queue.empty()
    path, value = ingress_queue.get_nowait()
    assert path == "Line_A.lathe.current_cap"
    assert value == 10


@pytest.mark.asyncio
@patch("dynamic_des.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_kafka_ingress_success(MockConsumer):
    """Verify the default JSON fallback deserializes successfully."""

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            class FakeMessage:
                topic = "test-topic"  # Required by the new topic router
                value = json.dumps(
                    {"path_id": "Line_A.resources.lathe.current_cap", "value": 5}
                ).encode("utf-8")

            yield FakeMessage()
            await asyncio.sleep(999)

    mock_consumer_instance = FakeConsumer()
    MockConsumer.return_value = mock_consumer_instance

    ingress_queue = queue.Queue()
    ingress = KafkaIngress(topic="test-topic", bootstrap_servers="localhost:9092")

    task = asyncio.create_task(ingress.run(ingress_queue))
    await asyncio.sleep(0.1)

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    mock_consumer_instance.start.assert_called_once()
    mock_consumer_instance.stop.assert_called_once()

    assert not ingress_queue.empty()
    path, value = ingress_queue.get_nowait()
    assert path == "Line_A.resources.lathe.current_cap"
    assert value == 5
