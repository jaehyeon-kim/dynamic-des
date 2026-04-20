import asyncio
import queue
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest
from pydantic import BaseModel

from dynamic_des.connectors.egress.kafka import (
    ConfluentAvroSerializer,
    GlueAvroSerializer,
    KafkaEgress,
    _extract_dict,
)


class DummyModel(BaseModel):
    """A dummy Pydantic model for duck-typing tests."""

    path_id: str
    value: int
    stream_type: str = "telemetry"


def test_extract_dict_helper():
    """Verify duck-typing helper extracts dicts from raw dicts and Pydantic models."""
    # 1. Raw dict
    raw = {"path_id": "A", "value": 1}
    assert _extract_dict(raw) == raw

    # 2. Pydantic V2 Model
    model = DummyModel(path_id="B", value=2)
    extracted = _extract_dict(model)
    assert extracted["path_id"] == "B"
    assert extracted["value"] == 2
    assert extracted["stream_type"] == "telemetry"


def test_missing_avro_dependencies():
    """Verify that using Avro serializers without the extras raises an explicit ImportError."""
    # Hide confluent_kafka
    with patch.dict(sys.modules, {"confluent_kafka": None}):
        with pytest.raises(ImportError, match=r"pip install dynamic-des\[confluent\]"):
            ConfluentAvroSerializer(registry_url="http://mock", schema_str="{}")

    # Hide boto3
    with patch.dict(sys.modules, {"boto3": None}):
        with pytest.raises(ImportError, match=r"pip install dynamic-des\[glue\]"):
            GlueAvroSerializer(registry_name="mock", schema_str="{}")


@pytest.mark.asyncio
@patch("dynamic_des.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_pluggable_serializers(MockProducer):
    """Verify that topic_serializers correctly route payloads to custom serializers."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    # Create a mock serializer that returns a specific byte signature
    mock_serializer = MagicMock()
    mock_serializer.serialize.return_value = b"MOCK_AVRO_BYTES"

    mock_egress_queue = queue.Queue()
    # We send two messages: one mapped to custom serializer, one falling back to JSON
    data_custom = {"stream_type": "event", "key": "123", "value": "avro_data"}
    data_json = {"stream_type": "telemetry", "path_id": "Line_A", "value": 5}

    mock_egress_queue.put([data_custom, data_json])

    # Instantiate with the custom serializer mapped to "sim-events"
    egress = KafkaEgress(
        bootstrap_servers="localhost:9092",
        telemetry_topic="sim-telemetry",
        event_topic="sim-events",
        topic_serializers={"sim-events": mock_serializer},
    )

    task = asyncio.create_task(egress.run(mock_egress_queue))
    await asyncio.sleep(0.1)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    assert mock_producer_instance.send.call_count == 2

    # 1. Verify custom serializer intercepted the event topic
    call_1 = mock_producer_instance.send.call_args_list[0]
    assert call_1.args[0] == "sim-events"
    assert call_1.kwargs["value"] == b"MOCK_AVRO_BYTES"
    mock_serializer.serialize.assert_called_once()

    # 2. Verify fallback JSON serializer handled the telemetry topic
    call_2 = mock_producer_instance.send.call_args_list[1]
    assert call_2.args[0] == "sim-telemetry"
    assert b"Line_A" in call_2.kwargs["value"]  # It should be raw JSON bytes


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

    mock_producer_instance.start.assert_called_once()
    mock_producer_instance.send.assert_called_once()
    mock_producer_instance.stop.assert_called_once()

    call_args = mock_producer_instance.send.call_args
    target_topic = call_args.args[0]
    sent_key = call_args.kwargs["key"]
    sent_payload_bytes = call_args.kwargs["value"]

    assert target_topic == telemetry_topic
    assert sent_key == b"Line_A.lathe.queue_length"

    decoded_payload = orjson.loads(sent_payload_bytes)
    assert "stream_type" not in decoded_payload
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
    assert sent_key == b"sim_start_event"

    decoded_payload = orjson.loads(sent_payload_bytes)
    assert "stream_type" not in decoded_payload


@pytest.mark.asyncio
@patch("dynamic_des.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_custom_topic_router(MockProducer):
    """Verify that a custom router function correctly overrides default behavior."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    def custom_topic_router(payload: dict) -> str:
        value = payload.get("value", {})
        if isinstance(value, dict):
            event_type = value.get("event_type")
            if event_type == "prediction_request":
                return "mill-predictions"
        return "mill-lifecycle"

    mock_egress_queue = queue.Queue()

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

    assert mock_producer_instance.send.call_count == 2

    call_1 = mock_producer_instance.send.call_args_list[0]
    assert call_1.args[0] == "mill-predictions"
    assert call_1.kwargs["key"] == b"req_123"

    call_2 = mock_producer_instance.send.call_args_list[1]
    assert call_2.args[0] == "mill-lifecycle"
    assert call_2.kwargs["key"] == b"req_456"

    decoded_payload_1 = orjson.loads(call_1.kwargs["value"])
    assert "stream_type" not in decoded_payload_1
