import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynamic_des.connectors.ingress.kafka import KafkaIngress


class MockMsg:
    """Simulates a Kafka Message object."""

    def __init__(self, value):
        self.value = value


def mock_async_iterator(items):
    """Helper to create a valid async iterator for mocks."""

    async def _gen():
        for item in items:
            yield item

    return _gen()


@pytest.mark.asyncio
async def test_kafka_ingress_success(mock_ingress_queue):
    """Verify KafkaIngress parses JSON and pushes to queue."""
    topic = "test_topic"
    servers = "localhost:9092"

    # Prepare the payload
    data = {"path_id": "Line_A.arrival.rate", "value": 0.5}
    raw_payload = json.dumps(data).encode("utf-8")
    mock_msg = MockMsg(raw_payload)

    # Setup Mock Consumer
    mock_consumer = AsyncMock()

    # Fix the Async Iterator
    # __aiter__ must be a regular MagicMock (sync) returning an async generator
    mock_consumer.__aiter__ = MagicMock(return_value=mock_async_iterator([mock_msg]))

    # Patch the AIOKafkaConsumer where it is used in the ingress module
    with patch(
        "dynamic_des.connectors.ingress.kafka.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        ingress = KafkaIngress(topic, servers)
        await ingress.run(mock_ingress_queue)

    # Verify results
    assert not mock_ingress_queue.empty()
    path_id, value = mock_ingress_queue.get_nowait()
    assert path_id == "Line_A.arrival.rate"
    assert value == 0.5

    assert mock_consumer.start.called
    assert mock_consumer.stop.called


@pytest.mark.asyncio
async def test_kafka_ingress_invalid_json(mock_ingress_queue, capsys):
    """Verify KafkaIngress handles malformed JSON without crashing."""
    mock_consumer = AsyncMock()
    mock_consumer.__aiter__ = MagicMock(
        return_value=mock_async_iterator([MockMsg(b"invalid-json")])
    )

    with patch(
        "dynamic_des.connectors.ingress.kafka.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        ingress = KafkaIngress("topic", "localhost")
        await ingress.run(mock_ingress_queue)

    # Queue should be empty because parsing failed
    assert mock_ingress_queue.empty()

    # Capture stdout to check for the error message
    captured = capsys.readouterr()
    assert "KafkaIngress Error" in captured.out
