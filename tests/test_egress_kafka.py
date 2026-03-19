import asyncio
import queue
from unittest.mock import AsyncMock, patch

import orjson
import pytest

from dynamic_des.connectors.egress.kafka import KafkaEgress


@pytest.mark.asyncio
@patch("dynamic_des.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_success(MockProducer):
    # Setup Mock Producer Instance
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    telemetry_topic = "metrics_topic"
    event_topic = "event_topic"
    servers = "localhost:9092"

    # Prepare queue and sample telemetry data
    mock_egress_queue = queue.Queue()
    data = {
        "stream_type": "telemetry",
        "path_id": "Line_A.lathe.queue_length",
        "value": 10,
        "sim_ts": 12.5,
        "timestamp": "2023-10-25T14:30:00.000Z",
    }
    mock_egress_queue.put([data])

    egress = KafkaEgress(
        telemetry_topic=telemetry_topic,
        event_topic=event_topic,
        bootstrap_servers=servers,
    )

    # Run in a task so we can cancel the infinite loop
    task = asyncio.create_task(egress.run(mock_egress_queue))

    # Give it a moment to process the queue
    await asyncio.sleep(0.1)

    # Trigger the clean shutdown
    task.cancel()
    try:
        await task  # Yield back to the loop so the CancelledError block runs
    except asyncio.CancelledError:
        pass

    # Verify Lifecycle
    mock_producer_instance.start.assert_called_once()
    mock_producer_instance.send.assert_called_once()
    mock_producer_instance.stop.assert_called_once()  # Proves teardown works!

    # Check Payload
    call_args = mock_producer_instance.send.call_args
    target_topic = call_args.args[0]
    sent_payload_bytes = call_args.kwargs["value"]

    assert target_topic == telemetry_topic

    # Use orjson to decode since the producer uses orjson.dumps
    decoded_payload = orjson.loads(sent_payload_bytes)
    assert decoded_payload["path_id"] == "Line_A.lathe.queue_length"
    assert decoded_payload["value"] == 10
