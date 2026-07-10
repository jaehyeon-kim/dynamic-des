import asyncio
import json
import queue
import pytest
from aiokafka import AIOKafkaConsumer

from dynamic_des.connectors.egress.kafka import KafkaEgress
from dynamic_des.connectors.admin.kafka import KafkaAdminConnector


@pytest.mark.asyncio
@pytest.mark.integration
async def test_kafka_egress_and_admin(kafka_container):
    """
    Tests that KafkaEgress successfully writes to a broker,
    and KafkaAdminConnector successfully creates topics and reads them.
    """
    bootstrap_servers = kafka_container

    # 1. Create Topics using Admin Connector
    admin = KafkaAdminConnector(bootstrap_servers)
    admin.create_topics([{"name": "test-events", "partitions": 1}])

    # 2. Write Data using KafkaEgress
    egress = KafkaEgress(bootstrap_servers, event_topic="test-events")
    q = queue.Queue()

    mock_batch = [
        {
            "stream_type": "event",
            "key": "order-1",
            "value": {"order_id": 1, "amount": 100.0},
        },
    ]

    q.put(mock_batch)
    task = asyncio.create_task(egress.run(q))
    await asyncio.sleep(2.0)
    task.cancel()

    # 3. Read Data using a standard consumer to verify it hit the broker
    consumer = AIOKafkaConsumer(
        "test-events",
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id="test-group",
    )

    await consumer.start()
    try:
        # Use a timeout to prevent hanging if it fails
        msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
        payload = json.loads(msg.value.decode("utf-8"))
        assert payload["key"] == "order-1"
        assert payload["value"]["amount"] == 100.0
    finally:
        await consumer.stop()
