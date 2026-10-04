"""Adapts the hot rolling processes to the calls a blueprint makes.

A blueprint calls every process as `function(context, **kwargs)`. The processes in
sim_logic.py take `(env, sampler, resource, ...)` instead, so each function here
takes the context, picks out what the process needs, and hands over with
`yield from`. Nothing in sim_logic.py changes.
"""

import time
import uuid

from dynamic_des import KafkaAdminConnector
from generator import telemetry_monitor
from sim_logic import arrival_process, drift_engine, roll_slab
from src.config import (
    KAFKA_BROKER,
    TOPIC_CONTROL_INGRESS,
    TOPIC_GROUND_TRUTH,
    TOPIC_LIFECYCLE,
    TOPIC_PREDICTION_REQUESTS,
    TOPIC_TELEMETRY,
)

# The command-line options of generator.py, fixed here.
VARIABLE_PASSES = False
MAX_PASSES = 5


def drift(context, product_lines):
    yield from drift_engine(context.env, product_lines)


def monitor(context, product_lines):
    yield from telemetry_monitor(context.env, product_lines)


def primer(context, product):
    """Rolls a first slab, so the mill does not start dry."""
    yield from roll_slab(
        context.env,
        uuid.uuid4().hex[:8].upper(),
        product,
        VARIABLE_PASSES,
        MAX_PASSES,
        context.sampler,
        context.get_resource(f"mill_{product}"),
    )


def arrivals(context, product):
    yield from arrival_process(
        context.env,
        product,
        VARIABLE_PASSES,
        MAX_PASSES,
        context.sampler,
        context.get_resource(f"mill_{product}"),
    )


def create_topics():
    """Creates the five topics generator.py creates before it starts."""
    KafkaAdminConnector(bootstrap_servers=KAFKA_BROKER).create_topics(
        topics_config=[
            {"name": TOPIC_CONTROL_INGRESS, "partitions": 1},
            {"name": TOPIC_TELEMETRY, "partitions": 1},
            {"name": TOPIC_LIFECYCLE, "partitions": 1},
            {"name": TOPIC_PREDICTION_REQUESTS, "partitions": 3},
            {"name": TOPIC_GROUND_TRUTH, "partitions": 3},
        ]
    )
    time.sleep(2)
