import asyncio
import json
import queue
from unittest.mock import AsyncMock, patch

import pytest

from dynamic_des.connectors.egress.kafka import KafkaEgress


@pytest.fixture
def mock_output_queue():
    return queue.Queue()


@pytest.mark.asyncio
async def test_kafka_egress_success(mock_output_queue):
    topic = "metrics_topic"
    servers = "localhost:9092"

    # Prepare sample telemetry data
    data = {"path_id": "Line_A.queue_length", "value": 10, "timestamp": 1234.5}
    mock_output_queue.put(data)

    # Setup Mock Producer
    mock_producer = AsyncMock()

    # Patch where used
    with patch(
        "dynamic_des.connectors.egress.kafka.AIOKafkaProducer",
        return_value=mock_producer,
    ):
        egress = KafkaEgress(topic, servers)

        # Run in a task so we can cancel the infinite loop
        task = asyncio.create_task(egress.run(mock_output_queue))

        # Give it a moment to process the queue
        await asyncio.sleep(0.2)
        task.cancel()

    # Verify
    assert mock_producer.start.called
    assert mock_producer.send_and_wait.called

    # Check the payload sent
    call_args = mock_producer.send_and_wait.call_args
    sent_topic, sent_payload = call_args[0]
    assert sent_topic == topic
    assert json.loads(sent_payload.decode("utf-8")) == data
