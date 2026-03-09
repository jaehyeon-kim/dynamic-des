import asyncio
from unittest.mock import AsyncMock, patch

import orjson
import pytest

from dynamic_des.connectors.egress.kafka import KafkaEgress


@pytest.mark.asyncio
async def test_kafka_egress_success(mock_egress_queue):
    telemetry_topic = "metrics_topic"
    event_topic = "event_topic"
    servers = "localhost:9092"

    # Prepare sample telemetry data
    data = {
        "stream_type": "telemetry",
        "path_id": "Line_A.lathe.queue_length",
        "value": 10,
        "timestamp": 1234.5,
    }
    mock_egress_queue.put([data])

    # Setup Mock Producer
    mock_producer = AsyncMock()

    # Patch where used
    with patch(
        "dynamic_des.connectors.egress.kafka.AIOKafkaProducer",
        return_value=mock_producer,
    ):
        egress = KafkaEgress(telemetry_topic, event_topic, servers)

        # Run in a task so we can cancel the infinite loop
        task = asyncio.create_task(egress.run(mock_egress_queue))

        # Give it a moment to process the queue
        await asyncio.sleep(0.2)
        task.cancel()

    # 4. Verify
    assert mock_producer.start.called
    assert mock_producer.send.called  # Verify send was called (aiokafka default)

    # Check payload
    call_args = mock_producer.send.call_args
    target_topic = call_args[0][0]
    sent_payload_bytes = call_args[1]["value"]

    assert target_topic == telemetry_topic
    # Use orjson to decode since the producer uses orjson.dumps
    assert orjson.loads(sent_payload_bytes)["path_id"] == "Line_A.lathe.queue_length"
