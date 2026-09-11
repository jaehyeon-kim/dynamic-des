"""Confluent Avro round trip against a real schema registry.

Unit tests mock the serializer, so they answer whether `KafkaEgress` calls it, not
whether it can register a schema and produce bytes another consumer can read. That
gap let a packaging defect reach a release. These tests close it by publishing
through Karapace and reading the result back three ways: the registry's own subject
list, the raw bytes on the topic, and the deserializer.

Asserting the wire framing matters as much as asserting the decoded value. A round
trip through one library can pass while the bytes are wrong, and the framing is what
Trino, Flink and ClickHouse read.
"""

import asyncio
import json
import queue
import urllib.request

import pytest
from aiokafka import AIOKafkaConsumer

from dynamic_des.connectors.admin.kafka import KafkaAdminConnector
from dynamic_des.connectors.egress.kafka import ConfluentAvroSerializer, KafkaEgress
from dynamic_des.connectors.ingress.kafka import ConfluentAvroDeserializer

TOPIC = "test-avro-events"

# KafkaEgress publishes the whole record minus `stream_type`, so the schema covers
# the record's own keys rather than just the payload under `value`.
SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "OrderEvent",
        "namespace": "dynamic_des.test",
        "fields": [
            {"name": "key", "type": "string"},
            {
                "name": "value",
                "type": {
                    "type": "record",
                    "name": "OrderValue",
                    "fields": [
                        {"name": "order_id", "type": "int"},
                        {"name": "amount", "type": "double"},
                    ],
                },
            },
        ],
    }
)

RECORD = {
    "stream_type": "event",
    "key": "order-1",
    "value": {"order_id": 1, "amount": 100.0},
}


async def _publish_and_consume_raw(
    bootstrap_servers: str, registry_url: str, group_id: str
) -> bytes:
    """Publish one Avro record and return the bytes that land on the topic.

    The egress task stays alive until the consumer has the message, rather than
    being cancelled after a fixed sleep. A sleep long enough on an idle machine is
    not long enough when the rest of the suite is competing for the same broker,
    and the producer connecting is what the wait is really for.
    """
    egress = KafkaEgress(
        bootstrap_servers,
        event_topic=TOPIC,
        topic_serializers={
            TOPIC: ConfluentAvroSerializer(registry_url, SCHEMA),
        },
    )

    q: queue.Queue = queue.Queue()
    q.put([RECORD])

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=group_id,
    )
    await consumer.start()
    task = asyncio.create_task(egress.run(q))
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=30.0)
        return message.value
    finally:
        task.cancel()
        await consumer.stop()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_confluent_avro_registers_subject_and_frames_payload(
    kafka_container, schema_registry
):
    """The serializer registers `<topic>-value` and produces Confluent-framed bytes."""
    KafkaAdminConnector(kafka_container).create_topics(
        [{"name": TOPIC, "partitions": 1}]
    )
    payload = await _publish_and_consume_raw(
        kafka_container, schema_registry, "avro-framing-group"
    )

    # The registry holds the subject under the default TopicNameStrategy.
    with urllib.request.urlopen(f"{schema_registry}/subjects", timeout=5) as response:
        subjects = json.loads(response.read())
    assert f"{TOPIC}-value" in subjects

    # Confluent framing: magic byte 0, then the schema id as four big-endian bytes.
    assert payload[0] == 0
    schema_id = int.from_bytes(payload[1:5], "big")
    assert schema_id > 0

    # That id resolves in the registry, and the schema it returns is the one sent.
    with urllib.request.urlopen(
        f"{schema_registry}/schemas/ids/{schema_id}", timeout=5
    ) as response:
        registered = json.loads(json.loads(response.read())["schema"])
    assert registered["name"] == "OrderEvent"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_confluent_avro_round_trip(kafka_container, schema_registry):
    """A record published through the serializer decodes back to what was sent."""
    KafkaAdminConnector(kafka_container).create_topics(
        [{"name": TOPIC, "partitions": 1}]
    )
    payload = await _publish_and_consume_raw(
        kafka_container, schema_registry, "avro-round-trip-group"
    )
    decoded = ConfluentAvroDeserializer(schema_registry).deserialize(TOPIC, payload)

    assert decoded["key"] == RECORD["key"]
    assert decoded["value"] == RECORD["value"]

    # `stream_type` routes the record to a topic and is not part of the message,
    # which is what `include_stream_type=False` means.
    assert "stream_type" not in decoded
