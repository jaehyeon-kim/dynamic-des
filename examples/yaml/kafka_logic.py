"""Python for examples/yaml/kafka.yaml: the task payload, telemetry and topic setup."""

import logging
import os
import time

from pydantic import BaseModel

from dynamic_des import KafkaAdminConnector

logger = logging.getLogger("kafka_example")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


class TaskEvent(BaseModel):
    """Strongly typed event payload, so every finished event has the same shape."""

    path_id: str
    status: str


def process_part(task_id: int, context):
    """The finished event of each task. The task itself is declared in the YAML."""
    logger.info(f"Task {task_id} started at sim time: {context.env.now:.2f}s")
    return TaskEvent(path_id="Line_A.service.milling", status="finished").model_dump(
        mode="json"
    )


def telemetry_monitor(context):
    """Low-volume system health stream."""
    res = context.get_resource("lathe")

    context.publish("lathe.capacity", res.capacity)
    context.publish("lathe.in_use", res.in_use)
    context.publish("lathe.queue_length", len(res.queue.items))

    util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
    context.publish("lathe.utilization", util)

    avg_wait = len(res.queue.items) * 3.0
    context.publish("lathe.avg_wait", avg_wait)


def create_topics():
    """Creates the three topics before the run starts."""
    logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    try:
        admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
        admin.create_topics(
            topics_config=[
                {"name": "sim-config", "partitions": 1},
                {"name": "sim-telemetry", "partitions": 1},
                {"name": "sim-events", "partitions": 1},
            ]
        )
        time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not explicitly create topics: {e}")
