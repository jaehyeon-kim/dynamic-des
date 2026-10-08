# Dynamic DES

[![CI Pipeline](https://github.com/jaehyeon-kim/dynamic-des/actions/workflows/pipeline.yml/badge.svg)](https://github.com/jaehyeon-kim/dynamic-des/actions/workflows/pipeline.yml)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://jaehyeon.me/dynamic-des/)
[![PyPI version](https://badge.fury.io/py/dynamic-des.svg)](https://badge.fury.io/py/dynamic-des)
[![Python Versions](https://img.shields.io/pypi/pyversions/dynamic-des.svg)](https://pypi.org/project/dynamic-des/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Real-time SimPy simulations that take parameter changes while they run and write their events and telemetry to streams, databases, files and Iceberg tables.**

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/docs/assets/architecture.png" alt="Dynamic DES architecture" width="900" />
</div>

Dynamic DES runs [SimPy](https://simpy.readthedocs.io/) discrete-event simulations in step with the system clock, or as fast as the machine allows. A running simulation takes parameter changes (arrival rates, service times, capacities) from **Kafka**, **Redis**, **PostgreSQL** or a timed scenario, without stopping. Its task events and telemetry go to the sinks you attach: **Kafka**, **Redis**, **PostgreSQL**, **Parquet** or **JSONL** files on local disk or **S3-compatible storage** such as AWS S3 or SeaweedFS, or an **Apache Iceberg** table through a REST catalog.

A simulation can be written three ways: with the low-level `DynamicRealtimeEnvironment`, with the declarative `SimulationContext` builder, or as a plain **YAML blueprint** run with the `ddes` command. One run can generate backdated history at full speed and then continue in real time, so the same model can fill a data lake and then feed a live system.

## Key Features

- **⚡ Real-Time Control**: Synchronize SimPy with the system clock using `DynamicRealtimeEnvironment`.
- **🧭 Three Ways to Write a Simulation**: The low-level `DynamicRealtimeEnvironment`, the declarative `SimulationContext` builder, or a YAML blueprint. All three build the same parameters and run on the same environment.
- **🧾 YAML Blueprints**: Declare parameters, connectors, tasks, telemetry and timed experiments in a plain YAML file and run it with `ddes run`. Logic that YAML cannot express stays in Python and is referenced through `!python`.
- **⏩ Backfill Then Go Live**: One run generates backdated history unpaced, then switches to real time at `go_live_at`, with one seed and one seam.
- **🔀 Several Sinks per Run**: Attach a stream sink and a lake sink to one run, each with its own `when` predicate, `batch_size` and `flush_interval` on `add_egress`.
- **🔗 Dynamic Registry**: Dynamic, path-based updates (e.g., `Line_A.arrival.standard.rate`) that trigger instant logic changes.
- **🚀 High Throughput**: Optimized to handle high throughput using `orjson` and local batching.
- **🛡️ Enterprise Ready**: Native `**kwargs` passthrough for SASL, mTLS, OAuth, and AWS IAM Kafka clusters.
- **📦 Pluggable Serialization**: Stream lightweight JSON by default, or map specific ML topics to lazy-loaded **Avro/Schema Registry** serializers (Confluent & AWS Glue).
- **🗄️ Data Lake Ingestion**: Native PyArrow VFS integration for fast chunked writing (Parquet/JSONL) directly to object storage, with built-in schema inference and drift enforcement.
- **🧊 Lakehouse Ingestion**: Append straight into an Apache Iceberg table through an Iceberg REST catalog, with one commit per flush so the snapshot count stays under your control.
- **🦆 Pydantic Duck-Typing**: Seamlessly publish strictly-typed Pydantic V2 models straight from your simulation logic.
- **📊 System Observability**: Built-in lag monitoring to track simulation drift from real-world time.
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

# For Lakehouse Storage (Apache Iceberg)
pip install "dynamic-des[iceberg]"

# For all backends (Kafka, Redis, Postgres, Avro, Parquet, Iceberg)
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
uv tool install "odctl>=1.0,<2"

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
pip install "dynamic-des[kafka,parquet]" "odctl>=1.0,<2" nicegui

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
| storage | `odctl up storage` | `*/parquet_example.py` with `USE_S3=true` |
| catalog | `odctl up catalog` | `declarative/iceberg_example.py`, `imperative/iceberg_example.py` |

Paths in that table are relative to the `examples/` folder. `declarative/local_example.py` needs no container, and `*/parquet_example.py` needs one only when `USE_S3=true`. The YAML blueprints in `examples/yaml/` need the same profile as their declarative twin.

Guide: [Backfill then live](https://jaehyeon.me/dynamic-des/latest/guides/backfill-then-live/).


The control dashboard lets you update simulation parameters live and watch the telemetry react without restarting the run:

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/docs/assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="800" />
</div>

## Three Ways to Write a Simulation

A simulation can be written with the low-level API, with the declarative API, or as a YAML blueprint. All three build the same `SimParameter` and run on the same `DynamicRealtimeEnvironment`, so the registry paths, the records and the connectors are the same whichever way you choose. [Ways to write a simulation](https://jaehyeon.me/dynamic-des/latest/architecture/overview/) compares them, and the tutorials build one factory in each: [Part 1: Low-level API](https://jaehyeon.me/dynamic-des/latest/tutorials/low-level/), [Part 2: Declarative API](https://jaehyeon.me/dynamic-des/latest/tutorials/declarative/) and [Part 3: YAML](https://jaehyeon.me/dynamic-des/latest/tutorials/yaml/).

### Declarative API

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

#### What this does

1.  **Declarative Builder**: `SimulationContext` chains the setup, defining parameters, connectors, and configuration in one clean block.
2.  **Live Ingress**: The `LocalIngress` schedules registry mutations independently from the simulation logic.
3.  **Automatic Task Lifecycle**: The `@app.task` decorator automatically handles queued/started/finished event emissions, resource locking, and random duration sampling.
4.  **Telemetry Egress**: The `@app.telemetry_loop` captures continuous stats and streams them to the designated egress (`ConsoleEgress`).

### Low-level API

The low-level API is `DynamicRealtimeEnvironment` used directly. A script registers a `SimParameter` with the registry, attaches connectors with `setup_ingress` and `setup_egress`, creates each `DynamicResource`, and starts plain SimPy processes with `env.process`. Use it when you need what the builder does not do, such as resources created mid-run. Every example in [`examples/imperative/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/imperative) is written this way. See the [Low-level API page](https://jaehyeon.me/dynamic-des/latest/architecture/low-level/).

### YAML

A simulation can also be a plain YAML blueprint: parameters, connectors, simple tasks, telemetry and a scenario of changes at set simulation times, with no Python. This is the local example as a blueprint:

```yaml title="examples/yaml/local.yaml"
# Local simulation in YAML, with no Python and no containers.
#
# The twin of examples/declarative/local_example.py. Factory_A writes to
# ConsoleEgress, so events and telemetry are printed to the terminal, and the run
# ends on its own after 60 simulation seconds.
#
# Run it with: ddes run examples/yaml/local.yaml

simulation:
  sim_id: Factory_A
  factor: 1.0

egress:
  - type: Console

resources:
  lathe: {current_cap: 2, max_cap: 5}

services:
  milling: {dist: normal, mean: 3.0, std: 0.5}

arrivals:
  # Each arrival spawns one process_part task.
  standard: {dist: exponential, rate: 1.0, spawn: process_part}

tasks:
  process_part:
    service: milling
    resource: lathe
    # The value of the task's finished event. id_field adds the task id as part_id.
    payload: {event_type: part_produced, quality: A}
    id_field: part_id

telemetry:
  # Samples the lathe every 2 simulation seconds.
  - interval: 2.0
    publish:
      utilization: lathe.utilization
      queue_length: lathe.queue_length

run:
  until: 60
```

The package installs a `ddes` command to run one:

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/local.yaml
ddes run local.yaml
```

`ddes run local.yaml --until 10` overrides `run.until`. From Python, `SimulationContext.from_yaml("local.yaml")` returns the built context.

Every declarative example has a YAML twin in [`examples/yaml/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml): `local.yaml`, `kafka.yaml`, `parquet.yaml`, `iceberg.yaml`, `postgres.yaml`, `redis.yaml` and `backfill_live.yaml`. They are plain YAML. `advanced/postgres_orders.yaml` keeps its order generator in Python, in `postgres_orders_logic.py` beside it, and references it with `!python`. See the [YAML Blueprints reference](https://jaehyeon.me/dynamic-des/latest/architecture/yaml/) and the [guide from a first file to connectors](https://jaehyeon.me/dynamic-des/latest/guides/yaml-blueprints/).

## Data Egress JSON Schemas

To ensure strict data contracts with external consumers (like Kafka, Redis, or PostgreSQL), `dynamic-des` publishes records in the shape of its `TelemetryPayload` and `EventPayload` models. Users can expect two distinct JSON structures depending on the stream type:

### Telemetry Stream

Used for scalar metrics like resource utilization, queue lengths, or simulation lag.

```json
{
  "stream_type": "telemetry",
  "path_id": "Line_A.resources.lathe.utilization",
  "value": 85.5,
  "sim_ts": 120.5,
  "timestamp": "2023-10-25T14:30:00.000"
}
```

### Event Stream

Used for discrete task lifecycle events (e.g., a part arriving, entering a queue, or finishing processing).

```json
{
  "stream_type": "event",
  "key": "task-001",
  "value": {
    "status": "finished",
    "path_id": "Line_A.service.milling"
  },
  "sim_ts": 125.0,
  "timestamp": "2023-10-25T14:30:04.500"
}
```

## More Examples

The [examples](./examples/) folder has the examples written three ways, in `imperative/`, `declarative/` and `yaml/`. Backfill then live has no `imperative/` version, and `yaml/advanced/` has one YAML-only example. Its README names what to install and which odctl profile each one needs.

## Core Concepts

**Dynamic DES** is built on the **Switchboard Pattern**, decoupling data sourcing from simulation logic.

### Switchboard Pattern

Instead of resources polling Kafka directly, the architecture is split into three layers:

1.  **Connectors (Ingress/Egress)**: Background threads handle the I/O: Kafka, Redis and PostgreSQL in both directions, and Parquet, JSONL and Iceberg for egress.
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

- **Tutorials**: [Part 1: Low-level API](https://jaehyeon.me/dynamic-des/latest/tutorials/low-level/), [Part 2: Declarative API](https://jaehyeon.me/dynamic-des/latest/tutorials/declarative/), [Part 3: YAML](https://jaehyeon.me/dynamic-des/latest/tutorials/yaml/).
- **Core Architecture**: [Overview](https://jaehyeon.me/dynamic-des/latest/architecture/overview/), [Low-level API](https://jaehyeon.me/dynamic-des/latest/architecture/low-level/), [Declarative API](https://jaehyeon.me/dynamic-des/latest/architecture/context/), [YAML Blueprints](https://jaehyeon.me/dynamic-des/latest/architecture/yaml/), and the runtime: [Realtime Environment](https://jaehyeon.me/dynamic-des/latest/architecture/environment/), [Registry and Live Parameters](https://jaehyeon.me/dynamic-des/latest/architecture/registry/), [Time](https://jaehyeon.me/dynamic-des/latest/architecture/time/), [Resources and Containers](https://jaehyeon.me/dynamic-des/latest/architecture/resources/), [Connectors](https://jaehyeon.me/dynamic-des/latest/architecture/connectors/), [Records and Telemetry](https://jaehyeon.me/dynamic-des/latest/architecture/records/), [Batching and Delivery](https://jaehyeon.me/dynamic-des/latest/architecture/batching/).
- **Guides**: [Backfill Then Go Live](https://jaehyeon.me/dynamic-des/latest/guides/backfill-then-live/), [Change Parameters While a Simulation Runs](https://jaehyeon.me/dynamic-des/latest/guides/live-parameters/), [YAML Blueprints](https://jaehyeon.me/dynamic-des/latest/guides/yaml-blueprints/), and the connector guides.
- **Examples**: one page per example, with the declarative, low-level and YAML versions in tabs, starting with the [local example](https://jaehyeon.me/dynamic-des/latest/examples/local/).

## Related reading

Blog posts about dynamic-des and the projects built on it are tagged [dynamic-des](https://jaehyeon.me/tags/dynamic-des/) on jaehyeon.me.

Projects that use dynamic-des:

- [benchtop](https://github.com/jaehyeon-kim/benchtop): hands-on data engineering and machine learning demos that run locally, most of them with data produced by dynamic-des.
- [agentic-analytics-system](https://github.com/jaehyeon-kim/agentic-analytics-system): conversational analytics over an Iceberg lakehouse, with the lakehouse data generated by dynamic-des.
- [oml-digital-twin-hotrolling](https://github.com/jaehyeon-kim/oml-digital-twin-hotrolling): a streaming digital twin of a steel hot rolling mill, with online machine learning on Kafka and Flink.

## License

MIT
