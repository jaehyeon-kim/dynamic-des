# Backfill Then Go Live (YAML)

This example builds the same simulation as [`examples/declarative/backfill_live_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/backfill_live_example.py) from a YAML blueprint. Until `go_live_at` the run is unpaced and writes backdated history to Parquet. From `go_live_at` it is paced in real time and publishes to Kafka. See [Backfill Then Go Live in One Run](../../guides/backfill-then-live.md) for how the two instants work.

The instants are computed from the current time, and the predicates compare against them, so both stay in `backfill_live_logic.py`.

---

## Quick Start

Download the blueprint and the Python module beside it into the same folder, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/backfill_live.yaml
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/backfill_live_logic.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Start the Kafka broker and schema registry
odctl up kafka-lite

# 3. Run the blueprint
uv run --no-project --with "dynamic-des[kafka,parquet]" dynamic-des run backfill_live.yaml

# 4. Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

### With pip

```bash
# 1. Install the package with the kafka,parquet extra, and odctl for the containers
pip install "dynamic-des[kafka,parquet]" "odctl>=0.5.1"

# 2. Start the Kafka broker and schema registry
odctl up kafka-lite

# 3. Run the blueprint
dynamic-des run backfill_live.yaml

# 4. Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

## What It Does

`run.before` calls `prepare`, which creates `data/backfill` and the two topics. Ten minutes of history are written to `data/backfill/` within the first second, then the run publishes to `sim-events` and `sim-telemetry` in real time for 60 seconds and ends. `HISTORY_MINUTES` and `LIVE_SECONDS` set the two halves, and `run.until` follows them because it is read from `backfill_live_logic.UNTIL`.

## Full Source Code

Each egress has a `when` predicate, the `!python` form of the `when=` argument of `add_egress`. `go_live_at` and `logical_start_time` take `datetime` values from the module.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/backfill_live.yaml"
# Backfill then go live in one run, in YAML.
#
# The twin of examples/declarative/backfill_live_example.py. Until go_live_at the
# clock is detached from the wall clock, so ten minutes of backdated history are
# written to Parquet as fast as the machine allows. From go_live_at the run is
# paced at one simulated second per real second, and the same events go to Kafka.
# The instants, the predicates and the router stay in backfill_live_logic.py
# beside this file, because they are computed from the current time.
#
# Needs a broker: odctl up kafka-lite.
# Run it with: dynamic-des run examples/yaml/backfill_live.yaml

simulation:
  sim_id: Line_A
  # Unpaced to begin with, so the history costs no real time.
  factor: 0.0
  random_seed: 42
  logical_start_time: !python backfill_live_logic.LOGICAL_START_TIME
  # From here the same run is paced at one simulated second per real second.
  go_live_at: !python backfill_live_logic.GO_LIVE_AT

egress:
  - type: Parquet
    config:
      path_router: !python backfill_live_logic.history_router
    when: !python backfill_live_logic.is_history
  - type: Kafka
    config:
      event_topic: sim-events
      telemetry_topic: sim-telemetry
      bootstrap_servers: !python backfill_live_logic.BOOTSTRAP_SERVERS
    when: !python backfill_live_logic.is_live

# Only batch_size governs the history, as in the Python twin. The interval flush
# starts at go-live, so the live tail also flushes every 10 seconds.
batching:
  batch_size: 2000
  flush_interval: 10.0

resources:
  lathe: {current_cap: 4, max_cap: 10}

services:
  milling: {dist: normal, mean: 2.0, std: 0.2}

arrivals:
  standard: {dist: exponential, rate: 0.5, spawn: process_part}

tasks:
  process_part:
    service: milling
    resource: lathe
    payload: {path_id: Line_A.service.milling, status: finished}

telemetry:
  - interval: 30.0
    publish:
      lathe.in_use: lathe.in_use
      lathe.queue_length: lathe.queue_length

run:
  # The history, then LIVE_SECONDS of live tail.
  until: !python backfill_live_logic.UNTIL
  before:
    - !python backfill_live_logic.prepare
```

```python title="examples/yaml/backfill_live_logic.py"
"""Python for examples/yaml/backfill_live.yaml: the instants, predicates and router."""

import logging
import os
import time
from datetime import datetime, timedelta

from dynamic_des import KafkaAdminConnector

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENT_TOPIC = "sim-events"
TELEMETRY_TOPIC = "sim-telemetry"

# How much history to generate, and how long to keep tailing once live. The live half
# costs real time, second for second, so it is short by default.
HISTORY = timedelta(minutes=float(os.getenv("HISTORY_MINUTES", "10")))
LIVE_SECONDS = float(os.getenv("LIVE_SECONDS", "60"))
UNTIL = HISTORY.total_seconds() + LIVE_SECONDS

base_path = os.getenv("DEST_PATH", "data/backfill")

# The go-live instant is now, so everything before it is history and everything after
# it is the live tail.
GO_LIVE_AT = datetime.now()
LOGICAL_START_TIME = GO_LIVE_AT - HISTORY

# Records carry their logical time as an ISO string, so the predicates that split the
# two sinks compare strings. That works because every timestamp comes from the same
# formatter: identical layout, so ordering by text is ordering by time.
GO_LIVE_ISO = GO_LIVE_AT.isoformat(timespec="milliseconds")


def is_history(record: dict) -> bool:
    """True for records stamped before the go-live instant."""
    return record["timestamp"] < GO_LIVE_ISO


def is_live(record: dict) -> bool:
    """True for records stamped at or after the go-live instant."""
    return record["timestamp"] >= GO_LIVE_ISO


def history_router(data: dict) -> str | None:
    """Drops telemetry and flattens the event payload, as Parquet needs flat rows."""
    if data.get("stream_type") != "event":
        return None

    if isinstance(data.get("value"), dict):
        data.update(data.pop("value"))

    return f"{base_path}/events.parquet"


def prepare():
    """Creates the history folder and the Kafka topics before the run starts."""
    os.makedirs(base_path, exist_ok=True)

    try:
        admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
        admin.create_topics(
            topics_config=[
                {"name": EVENT_TOPIC, "partitions": 1},
                {"name": TELEMETRY_TOPIC, "partitions": 1},
            ]
        )
        time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not explicitly create topics: {e}")

    logger.info(
        "Backfilling from %s to %s into '%s/', then tailing live to Kafka for %.0fs.",
        LOGICAL_START_TIME.strftime("%Y-%m-%d %H:%M:%S"),
        GO_LIVE_AT.strftime("%Y-%m-%d %H:%M:%S"),
        base_path,
        LIVE_SECONDS,
    )
```
