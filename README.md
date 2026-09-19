# Dynamic DES

[![CI Pipeline](https://github.com/jaehyeon-kim/dynamic-des/actions/workflows/pipeline.yml/badge.svg)](https://github.com/jaehyeon-kim/dynamic-des/actions/workflows/pipeline.yml)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://jaehyeon.me/dynamic-des/)
[![PyPI version](https://badge.fury.io/py/dynamic-des.svg)](https://badge.fury.io/py/dynamic-des)
[![Python Versions](https://img.shields.io/pypi/pyversions/dynamic-des.svg)](https://pypi.org/project/dynamic-des/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Real-time SimPy control plane for event-driven digital twins.**

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/docs/assets/architecture.png" alt="Dynamic DES architecture" width="900" />
</div>

Dynamic DES bridges the gap between static discrete-event simulations and the live world. It allows you to update simulation parameters (arrivals, service times, capacities) and stream telemetry via **Kafka**, **Redis**, or **PostgreSQL** without stopping the simulation. Beyond live streaming, it transforms static models into **synchronized forecasting engines**, enabling rapid historical data generation and future state prediction. Export compressed, chunked datasets (Parquet, JSONL) directly to local storage, **AWS S3, Google Cloud Storage, Azure Blob, or SeaweedFS** using PyArrow VFS, complete with strict schema drift prevention.

## Key Features

- **⚡ Real-Time Control**: Synchronize SimPy with the system clock using `DynamicRealtimeEnvironment`.
- **🔗 Dynamic Registry**: Dynamic, path-based updates (e.g., `Line_A.arrival.rate`) that trigger instant logic changes.
- **🚀 High Throughput**: Optimized to handle high throughput using `orjson` and local batching.
- **🛡️ Enterprise Ready**: Native `**kwargs` passthrough for SASL, mTLS, OAuth, and AWS IAM Kafka clusters.
- **📦 Pluggable Serialization**: Stream lightweight JSON by default, or map specific ML topics to lazy-loaded **Avro/Schema Registry** serializers (Confluent & AWS Glue).
- **🗄️ Data Lake Ingestion**: Native PyArrow VFS integration for fast chunked writing (Parquet/JSONL) directly to object storage, with built-in schema inference and drift enforcement.
- **🦆 Pydantic Duck-Typing**: Seamlessly publish strictly-typed Pydantic V2 models straight from your simulation logic.
- **🌍 Domain Agnostic**: Perfect for factory floors, crypto trading bots, or RPG game state management.

## Installation

Install the core library:

```bash
pip install dynamic-des
```

To include specific backends and enterprise features:

```bash
# For Kafka support
pip install "dynamic-des[kafka]"

# For Confluent Schema Registry (Avro)
pip install "dynamic-des[kafka,confluent]"

# For AWS Glue Schema Registry (Avro)
pip install "dynamic-des[kafka,glue]"

# For Redis support
pip install "dynamic-des[redis]"

# For PostgreSQL support
pip install "dynamic-des[postgres]"

# For Data Lake Storage (Parquet & PyArrow VFS)
pip install "dynamic-des[parquet]"

# For all backends (Kafka, Redis, Postgres, Avro, Parquet)
pip install "dynamic-des[all]"
```

## Quick Start: Running an Example

Dynamic DES ships runnable examples in the [`examples/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) folder of this repository. Download the ones you want, then run them.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/local_example.py
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/kafka_example.py
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/kafka_dashboard.py
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/backfill_live_example.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Local, dependency-free simulation
uv run --no-project --with dynamic-des local_example.py

# 3. Start the Kafka broker and schema registry (requires Docker)
odctl up kafka-lite

# 4. Run the real-time digital twin (Ctrl + C to stop)
uv run --no-project --with "dynamic-des[kafka]" kafka_example.py

# 5. In a second terminal, watch and steer the run from the dashboard. It serves
#    http://localhost:8080 rather than opening a browser. Ctrl + C to stop.
uv run --no-project --with "dynamic-des[kafka]" --with nicegui kafka_dashboard.py

# 6. Backfill ten minutes of history to Parquet, generated instantly rather than
#    waited for, then tail live to Kafka for sixty seconds
uv run --no-project --with "dynamic-des[kafka,parquet]" backfill_live_example.py

# 7. Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

### With pip

```bash
# 1. Install the package with both extras, odctl for the containers and
#    nicegui for the dashboard
pip install "dynamic-des[kafka,parquet]" "odctl>=0.5.1" nicegui

# 2. Local, dependency-free simulation
python local_example.py

# 3. Start the Kafka broker and schema registry (requires Docker)
odctl up kafka-lite

# 4. Run the real-time digital twin (Ctrl + C to stop)
python kafka_example.py

# 5. In a second terminal, watch and steer the run from the dashboard. It serves
#    http://localhost:8080 rather than opening a browser. Ctrl + C to stop.
python kafka_dashboard.py

# 6. Backfill ten minutes of history to Parquet, generated instantly rather than
#    waited for, then tail live to Kafka for sixty seconds
python backfill_live_example.py

# 7. Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

Examples that need a broker, a database or an object store get their container from [odctl](https://github.com/jaehyeon-kim/odctl). Start the profile an example needs before you run it, and stop it with `odctl down <profile> --volumes` when you are finished. Kafka and Redis are the two whose odctl profile names differ, because odctl ships a one-broker Kafka as `kafka-lite` and uses Valkey rather than Redis.

| Profile | Start | Needed by |
|---|---|---|
| kafka-lite | `odctl up kafka-lite` | `declarative/kafka_example.py`, `imperative/kafka_example.py`, `declarative/backfill_live_example.py`, `kafka_dashboard.py` |
| postgres | `odctl up postgres` | `declarative/postgres_example.py`, `imperative/postgres_example.py` |
| valkey | `odctl up valkey` | `declarative/redis_example.py`, `imperative/redis_example.py` |
| storage | `odctl up storage` | `declarative/history_example.py` with `USE_S3=true` |

Paths in that table are relative to the `examples/` folder. `declarative/local_example.py` needs no container, and `declarative/history_example.py` needs one only when `USE_S3=true`.

Guide: [Backfill then live](https://jaehyeon.me/dynamic-des/latest/guides/backfill-then-live/).


The control dashboard lets you update simulation parameters live and watch the telemetry react without restarting the run:

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/docs/assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="800" />
</div>

## Building Your Own Simulation (Local Example)

The following snippet demonstrates a simple example using the declarative **Standard API (`SimulationContext`)**. It initializes a production line, schedules dynamic capacity updates, and streams telemetry to the console.

```python
import logging
from dynamic_des import SimulationContext, ConsoleEgress, LocalIngress

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# 1. Initialize SimulationContext (Builder Pattern)
# Schedule capacity to jump to 3 at t=10s, then drop to 2 at t=20s
app = (
    SimulationContext(sim_id="Line_A", factor=1.0, random_seed=42)
    .add_resource("lathe", current_cap=1, max_cap=5)
    .add_arrival("standard", dist="exponential", rate=1.0)
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    .add_ingress(LocalIngress(
        schedule=[
            (10.0, "Line_A.resources.lathe.current_cap", 3),
            (20.0, "Line_A.resources.lathe.current_cap", 2),
        ]
    ))
    .add_egress(ConsoleEgress())
)

# 2. Define Simulation Processes using Decorators
@app.arrival_loop("standard")
def arrival_process(context: SimulationContext):
    task_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(work_task(task_id))
        task_id += 1

@app.task(service_id="milling", resource_id="lathe")
def work_task(task_id: int):
    # Returns custom metadata payload to be included in the finished event
    return {"part_id": task_id}

@app.telemetry_loop(interval=2.0)
def telemetry_monitor(context: SimulationContext):
    # Retrieve active resource handles to query state
    res = context.get_resource("lathe")
    context.env.publish_telemetry("Line_A.lathe.capacity", res.capacity)
    context.env.publish_telemetry("Line_A.lathe.in_use", res.in_use)
    context.env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))

# 3. Run the Simulation
print("Simulation started. Watch capacity change at t=10s and t=20s...")
app.run(until=25.0)
```

### What this does

1.  **Declarative Builder**: `SimulationContext` chains the setup, defining parameters, connectors, and configuration in one clean block.
2.  **Live Ingress**: The `LocalIngress` schedules registry mutations independently from the simulation logic.
3.  **Automatic Task Lifecycle**: The `@app.task` decorator automatically handles queued/started/finished telemetry emissions, resource locking, and random duration sampling.
4.  **Telemetry Egress**: The `@app.telemetry_loop` captures continuous stats and streams them to the designated egress (`ConsoleEgress`).

### Data Egress JSON Schemas

To ensure strict data contracts with external consumers (like Kafka, Redis, or PostgreSQL), `dynamic-des` uses Pydantic to validate all outbound payloads. Users can expect two distinct JSON structures depending on the stream type:

#### Telemetry Stream

Used for scalar metrics like resource utilization, queue lengths, or simulation lag.

```json
{
  "stream_type": "telemetry",
  "path_id": "Line_A.resources.lathe.utilization",
  "value": 85.5,
  "sim_ts": 120.5,
  "timestamp": "2023-10-25T14:30:00.000Z"
}
```

#### Event Stream

Used for discrete task lifecycle events (e.g., a part arriving, entering a queue, or finishing processing).

```json
{
  "stream_type": "event",
  "key": "task-001",
  "value": {
    "status": "finished",
    "duration": 45.2,
    "path_id": "Line_A.service.lathe"
  },
  "sim_ts": 125.0,
  "timestamp": "2023-10-25T14:30:04.500Z"
}
```

### More Examples

For more examples, including implementations using **Kafka** providers, please explore the [examples](./examples/) folder, which has its own README naming what to install and which odctl profile each one needs.

## Core Concepts

**Dynamic DES** is built on the **Switchboard Pattern**, decoupling data sourcing from simulation logic.

### Switchboard Pattern

Instead of resources polling Kafka directly, the architecture is split into three layers:

1.  **Connectors (Ingress/Egress)**: Background threads handle heavy I/O (Kafka, Redis).
2.  **Registry (Switchboard)**: A centralized state manager that flattens data into dot-notation paths.
3.  **Resources (SimPy Objects)**: Passive observers that "wake up" only when the Registry signals a change.

### Event-Driven Capacity

Standard SimPy resources have static capacities. `DynamicResource` wraps a `Container` and a `PriorityStore`. When the Registry updates:

- **Growing**: Extra tokens are added to the pool immediately.
- **Shrinking**: The resource requests tokens back. If they are busy, it waits until they are released, ensuring no work-in-progress is lost.

### High-Throughput Events

To handle high throughput, the `EgressMixIn` uses:

- **Batching**: Pushing lists of events to the I/O thread to reduce lock contention.
- **orjson**: Rust-powered serialization for maximum speed.

## Documentation

For full documentation, architecture details, and API reference, visit:
[https://jaehyeon.me/dynamic-des/](https://jaehyeon.me/dynamic-des/).

## Related reading

Blog posts that use this library:

- [Building a Real-Time Industrial Digital Twin with Apache Flink and Online Machine Learning](https://jaehyeon.me/blog/2026-04-21-digital-twin-online-machine-learning/): a Flink and Kotlin pipeline that detects concept drift in a hot strip mill and corrects for roller wear.
- [Why Digital Twins Are Rewiring Industry 4.0](https://jaehyeon.me/blog/2026-04-23-digital-twin-industry-4-0/): the architectural layers that separate traditional simulations, operational twins and event-driven hybrid pipelines.
- [Building an Event-Driven Hybrid Digital Twin with dynamic-des](https://jaehyeon.me/blog/2026-04-28-digital-twin-dynamic-des/): the Switchboard pattern, mutable resources and dynamic topic routing behind this library.
- [One Simulation, Two Pipelines: Batch Training and Live Inference with Dynamic DES v0.8.1](https://jaehyeon.me/blog/2026-05-25-dynamic-des-parquet-support/): one SimPy codebase that writes batch Parquet for training and streams live Kafka events for inference.
- [Dynamic DES v0.11.1: A Declarative API with Postgres and Redis Connectors](https://jaehyeon.me/blog/2026-07-17-dynamic-des-declarative-connectors/): what the declarative `SimulationContext` API changed, and the Postgres and Redis connectors it added.
- [Building an Agentic Analytics System over an Iceberg Lakehouse](https://jaehyeon.me/blog/2026-07-18-agentic-analytics-system/): a local analytics stack that uses this library to generate the lakehouse data an agent queries.

## License

MIT
