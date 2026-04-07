import asyncio
import queue
from unittest.mock import AsyncMock, patch

import orjson
import pytest

from dynamic_des.connectors.egress.kafka import KafkaEgress


@pytest.mark.asyncio
@patch("dynamic_des.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_default_telemetry_routing(MockProducer):
    """Verify default routing for telemetry streams."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    telemetry_topic = "metrics_topic"
    event_topic = "event_topic"
    servers = "localhost:9092"

    mock_egress_queue = queue.Queue()
    data = {
        "stream_type": "telemetry",
        "path_id": "Line_A.lathe.queue_length",
        "value": 10,
    }
    mock_egress_queue.put([data])

    egress = KafkaEgress(
        telemetry_topic=telemetry_topic,
        event_topic=event_topic,
        bootstrap_servers=servers,
    )

    task = asyncio.create_task(egress.run(mock_egress_queue))
    await asyncio.sleep(0.1)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify Lifecycle
    mock_producer_instance.start.assert_called_once()
    mock_producer_instance.send.assert_called_once()
    mock_producer_instance.stop.assert_called_once()

    # Check Routing and Key Extraction
    call_args = mock_producer_instance.send.call_args
    target_topic = call_args.args[0]
    sent_key = call_args.kwargs["key"]
    sent_payload_bytes = call_args.kwargs["value"]

    assert target_topic == telemetry_topic
    assert sent_key == b"Line_A.lathe.queue_length"  # Extracted from path_id

    # Check Payload Sanitization
    decoded_payload = orjson.loads(sent_payload_bytes)
    assert "stream_type" not in decoded_payload  # Should be popped
    assert decoded_payload["path_id"] == "Line_A.lathe.queue_length"
    assert decoded_payload["value"] == 10


@pytest.mark.asyncio
@patch("dynamic_des.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_default_event_routing(MockProducer):
    """Verify default routing for event streams."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    mock_egress_queue = queue.Queue()
    data = {
        "stream_type": "event",
        "key": "sim_start_event",
        "value": {"event_type": "lifecycle"},
    }
    mock_egress_queue.put([data])

    egress = KafkaEgress(
        telemetry_topic="metrics",
        event_topic="events",
        bootstrap_servers="localhost:9092",
    )

    task = asyncio.create_task(egress.run(mock_egress_queue))
    await asyncio.sleep(0.1)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    call_args = mock_producer_instance.send.call_args
    target_topic = call_args.args[0]
    sent_key = call_args.kwargs["key"]
    sent_payload_bytes = call_args.kwargs["value"]

    assert target_topic == "events"
    assert sent_key == b"sim_start_event"  # Extracted from standard key

    decoded_payload = orjson.loads(sent_payload_bytes)
    assert "stream_type" not in decoded_payload


@pytest.mark.asyncio
@patch("dynamic_des.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_custom_topic_router(MockProducer):
    """Verify that a custom router function correctly overrides default behavior."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    # Define the custom router exactly as it would be used in production
    def custom_topic_router(payload: dict) -> str:
        value = payload.get("value", {})
        if isinstance(value, dict):
            event_type = value.get("event_type")
            if event_type == "prediction_request":
                return "mill-predictions"
        return "mill-lifecycle"

    mock_egress_queue = queue.Queue()

    # We will send TWO messages in the batch to verify it routes both correctly
    data_1 = {
        "stream_type": "event",
        "key": "req_123",
        "value": {"event_type": "prediction_request", "data": [1, 2, 3]},
    }
    data_2 = {
        "stream_type": "event",
        "key": "req_456",
        "value": {"event_type": "random_status", "data": []},
    }

    mock_egress_queue.put([data_1, data_2])

    # Instantiate with the router
    egress = KafkaEgress(
        bootstrap_servers="localhost:9092", topic_router=custom_topic_router
    )

    task = asyncio.create_task(egress.run(mock_egress_queue))
    await asyncio.sleep(0.1)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify both messages were sent
    assert mock_producer_instance.send.call_count == 2

    # Verify first message routing (prediction_request)
    call_1 = mock_producer_instance.send.call_args_list[0]
    assert call_1.args[0] == "mill-predictions"
    assert call_1.kwargs["key"] == b"req_123"

    # Verify second message routing (fallback to mill-lifecycle)
    call_2 = mock_producer_instance.send.call_args_list[1]
    assert call_2.args[0] == "mill-lifecycle"
    assert call_2.kwargs["key"] == b"req_456"

    # Verify stream_type was still popped even when using custom router
    decoded_payload_1 = orjson.loads(call_1.kwargs["value"])
    assert "stream_type" not in decoded_payload_1
