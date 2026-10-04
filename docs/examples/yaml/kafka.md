# Kafka Digital Twin (YAML)

This example builds the same simulation as the [declarative Kafka example](../declarative/kafka.md) from a YAML blueprint. The connectors, the parameters and the task are in YAML. The task's Pydantic payload and a telemetry loop that derives a metric stay in `kafka_logic.py`, which the blueprint references with `!python`.

---

## Quick Start

Download the blueprint and the Python module beside it into the same folder, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/kafka.yaml
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/kafka_logic.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Start the Kafka broker and schema registry
odctl up kafka-lite

# 3. Run the blueprint (Ctrl + C to stop)
uv run --no-project --with "dynamic-des[kafka]" dynamic-des run kafka.yaml

# 4. Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

### With pip

```bash
# 1. Install the package with the kafka extra, and odctl for the containers
pip install "dynamic-des[kafka]" "odctl>=0.5.1"

# 2. Start the Kafka broker and schema registry
odctl up kafka-lite

# 3. Run the blueprint (Ctrl + C to stop)
dynamic-des run kafka.yaml

# 4. Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

## What It Does

The run keeps going until you stop it. `run.before` calls `create_topics` first, which creates `sim-config`, `sim-events` and `sim-telemetry`. After that the run logs one line per task as the task claims the lathe, publishes task lifecycle events to `sim-events` and lathe metrics to `sim-telemetry`, and applies parameter updates sent to `sim-config`.

The dashboard from the declarative example works with this run unchanged, because the topics and the records are the same: `uv run --no-project --with "dynamic-des[kafka]" --with nicegui kafka_dashboard.py`, after downloading [`kafka_dashboard.py`](https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/kafka_dashboard.py).

`KAFKA_BOOTSTRAP_SERVERS` overrides the broker address, because the blueprint reads it from `kafka_logic.BOOTSTRAP_SERVERS`.

## Full Source Code

The blueprint wires both Kafka connectors and declares the lathe, the service, the arrival and the task. The Python module holds what YAML cannot express: a payload built from a Pydantic model, a telemetry loop that computes `avg_wait` from the queue, and the topic setup.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/kafka.yaml"
# Kafka digital twin in YAML.
#
# The twin of examples/declarative/kafka_example.py. KafkaIngress reads parameter
# updates from sim-config, and KafkaEgress publishes lifecycle events to sim-events
# and metrics to sim-telemetry. The task payload and the telemetry loop stay in
# kafka_logic.py beside this file.
#
# Needs a broker: odctl up kafka-lite. Runs until interrupted with Ctrl + C.
# Run it with: dynamic-des run examples/yaml/kafka.yaml

simulation:
  sim_id: Line_A
  factor: 1.0
  random_seed: 42

ingress:
  - type: Kafka
    config:
      topic: sim-config
      bootstrap_servers: !python kafka_logic.BOOTSTRAP_SERVERS

egress:
  - type: Kafka
    config:
      event_topic: sim-events
      telemetry_topic: sim-telemetry
      bootstrap_servers: !python kafka_logic.BOOTSTRAP_SERVERS

resources:
  lathe: {current_cap: 1, max_cap: 10}

services:
  milling: {dist: normal, mean: 3.0, std: 0.5}

arrivals:
  standard: {dist: exponential, rate: 1.0, spawn: process_part}

tasks:
  process_part:
    service: milling
    resource: lathe
    # Returns a Pydantic model dumped to JSON, so the event has a declared shape.
    payload: !python kafka_logic.process_part

telemetry:
  # avg_wait is derived from the queue, so this loop is Python.
  - interval: 2.0
    function: !python kafka_logic.telemetry_monitor

run:
  before:
    - !python kafka_logic.create_topics
```

```python title="examples/yaml/kafka_logic.py"
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
```
