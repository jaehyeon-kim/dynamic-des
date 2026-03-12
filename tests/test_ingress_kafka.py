import asyncio
import json
import queue
from unittest.mock import AsyncMock, patch

import pytest

from dynamic_des.connectors.ingress.kafka import KafkaIngress


@pytest.mark.asyncio
@patch("dynamic_des.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_kafka_ingress_success(MockConsumer):

    # Create a foolproof fake consumer class
    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            # Fake message object inside the iterator
            class FakeMessage:
                value = json.dumps(
                    {"path_id": "Line_A.resources.lathe.current_cap", "value": 5}
                ).encode("utf-8")

            yield FakeMessage()
            await asyncio.sleep(999)  # Block so it doesn't immediately exit the loop

    mock_consumer_instance = FakeConsumer()
    MockConsumer.return_value = mock_consumer_instance

    # Setup Ingress
    ingress_queue = queue.Queue()
    ingress = KafkaIngress(topic="test-topic", bootstrap_servers="localhost:9092")

    # Run the background task
    task = asyncio.create_task(ingress.run(ingress_queue))

    # Give the event loop a moment to process the message
    await asyncio.sleep(0.1)

    # Trigger the clean shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify Lifecycle
    mock_consumer_instance.start.assert_called_once()
    mock_consumer_instance.stop.assert_called_once()

    # Check the Queue output
    assert not ingress_queue.empty()
    path, value = ingress_queue.get_nowait()
    assert path == "Line_A.resources.lathe.current_cap"
    assert value == 5
