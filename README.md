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

The control dashboard changes simulation parameters while a run is going, and the telemetry reacts without a restart:

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/docs/assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="800" />
</div>

## Key Features

- **Real time or full speed**: `DynamicRealtimeEnvironment` runs SimPy in step with the system clock, or unpaced, and reports how far the simulation lags behind real time.
- **Live parameters**: arrival rates, service times and capacities change mid-run through registry paths such as `Line_A.arrival.standard.rate`. A resource grows at once, and shrinks only as busy units are released, so no work in progress is lost.
- **Several sinks per run**: each sink added with `add_egress` has its own `when` filter, `batch_size` and `flush_interval`, so one run can feed a stream and a data lake together.
- **Backfill then go live**: one run generates backdated history at full speed, then switches to real time at `go_live_at`.
- **Three ways to write a simulation**: the low-level `DynamicRealtimeEnvironment`, the declarative `SimulationContext` builder, or a YAML blueprint run with `ddes run`.
- **Serialisation**: JSON through `orjson` by default, Avro through the Confluent or AWS Glue Schema Registry for chosen topics, and Pydantic models published as they are.
- **Kafka security**: extra keyword arguments go to the Kafka client, so SASL, mTLS, OAuth and AWS IAM clusters work.

## Installation

```bash
pip install dynamic-des
```

The connectors are optional extras, for example `pip install "dynamic-des[kafka,parquet]"`:

| Extra | Adds |
|---|---|
| `kafka` | Kafka ingress and egress |
| `confluent` | Avro with the Confluent Schema Registry (includes `kafka`) |
| `glue` | Avro with the AWS Glue Schema Registry (includes `kafka`) |
| `redis` | Redis ingress and egress |
| `postgres` | PostgreSQL ingress and egress |
| `parquet` | Parquet and JSONL files on local disk or S3-compatible storage |
| `iceberg` | Apache Iceberg tables through a REST catalog |
| `all` | Every extra above |

## Quick Start: Running an Example

Dynamic DES ships runnable examples in the [`examples/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) folder of this repository. Download the ones you want, then run them with [uv](https://docs.astral.sh/uv/).

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/local_example.py
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/kafka_example.py
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/kafka_dashboard.py
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/backfill_live_example.py
```

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

With pip, install `"dynamic-des[kafka,parquet]" "odctl>=1.0,<2" nicegui` once and run each file with `python`.

Examples that need a broker, a database or an object store get their container from [odctl](https://github.com/jaehyeon-kim/odctl). Start the profile an example needs before you run it, and stop it with `odctl down <profile> --volumes` when you are finished. Kafka and Redis are the two whose odctl profile names differ, because odctl ships a one-broker Kafka as `kafka-lite` and uses Valkey rather than Redis.

| Profile | Start | Needed by |
|---|---|---|
| kafka-lite | `odctl up kafka-lite` | `declarative/kafka_example.py`, `imperative/kafka_example.py`, `declarative/backfill_live_example.py`, `kafka_dashboard.py` |
| postgres | `odctl up postgres` | `declarative/postgres_example.py`, `imperative/postgres_example.py` |
| valkey | `odctl up valkey` | `declarative/redis_example.py`, `imperative/redis_example.py` |
| storage | `odctl up storage` | `*/parquet_example.py` with `USE_S3=true` |
| catalog | `odctl up catalog` | `declarative/iceberg_example.py`, `imperative/iceberg_example.py` |

Paths in that table are relative to the `examples/` folder. `declarative/local_example.py` needs no container, and `*/parquet_example.py` needs one only when `USE_S3=true`. The YAML blueprints in `examples/yaml/` need the same profile as their declarative twin. The [examples README](./examples/README.md) lists what each example installs and needs.

## Three Ways to Write a Simulation

A simulation can be written with the low-level API, with the declarative API, or as a YAML blueprint. All three build the same `SimParameter` and run on the same `DynamicRealtimeEnvironment`, so the registry paths, the records and the connectors are the same whichever way you choose. The [overview](https://jaehyeon.me/dynamic-des/latest/architecture/overview/) compares them.

This declarative example builds a production line with `SimulationContext`, changes the lathe capacity twice while it runs, and prints events and telemetry to the console:

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

`@app.task` emits the queued, started and finished events, takes and releases the resource and samples the service time. `LocalIngress` changes the lathe capacity at 10 and 20 simulation seconds, and `@app.telemetry_loop` reports the capacity, the units in use and the queue length every 2 simulation seconds.

The same kind of model can be a YAML file with no Python, such as [`examples/yaml/local.yaml`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/yaml/local.yaml), run with `ddes run local.yaml`. Every declarative example has a YAML twin in [`examples/yaml/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml), and the low-level versions are in [`examples/imperative/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/imperative).

## Roadmap

- **Planned for 1.0:** routing between tasks, entity state, more distributions, stores and containers, built-in statistics and `ddes explain`; a simulation server (`ddes serve`) with a REST API, a browser UI and schedules, run locally through [odctl](https://github.com/jaehyeon-kim/odctl); Concepts docs on the simulation building blocks.
- **Future work:** breakdowns and schedules, transport on maps, data-driven inputs, experiments and analysis, animation, generated values in YAML, more sinks and delivery faults.

The [roadmap page](https://jaehyeon.me/dynamic-des/latest/about/roadmap/) has the details.

## Learn More

The documentation is at [jaehyeon.me/dynamic-des](https://jaehyeon.me/dynamic-des/).

- **Tutorials**: one factory built three times, with the [low-level API](https://jaehyeon.me/dynamic-des/latest/tutorials/low-level/), the [declarative API](https://jaehyeon.me/dynamic-des/latest/tutorials/declarative/) and [YAML](https://jaehyeon.me/dynamic-des/latest/tutorials/yaml/).
- **Architecture**: the [overview](https://jaehyeon.me/dynamic-des/latest/architecture/overview/), the [registry](https://jaehyeon.me/dynamic-des/latest/architecture/registry/), [resources](https://jaehyeon.me/dynamic-des/latest/architecture/resources/), [connectors](https://jaehyeon.me/dynamic-des/latest/architecture/connectors/) and the [record formats](https://jaehyeon.me/dynamic-des/latest/architecture/records/) the sinks receive.
- **Guides**: [backfill then go live](https://jaehyeon.me/dynamic-des/latest/guides/backfill-then-live/), [changing parameters while a simulation runs](https://jaehyeon.me/dynamic-des/latest/guides/live-parameters/), [YAML blueprints](https://jaehyeon.me/dynamic-des/latest/guides/yaml-blueprints/) and the connector guides.
- **Examples**: one page per example, with the declarative, low-level and YAML versions side by side, starting with the [local example](https://jaehyeon.me/dynamic-des/latest/examples/local/).

## Related reading

Blog posts about dynamic-des and the projects built on it are tagged [dynamic-des](https://jaehyeon.me/tags/dynamic-des/) on jaehyeon.me.

Projects that use dynamic-des:

- [benchtop](https://github.com/jaehyeon-kim/benchtop): hands-on data engineering and machine learning demos that run locally, most of them with data produced by dynamic-des.
- [agentic-analytics-system](https://github.com/jaehyeon-kim/agentic-analytics-system): conversational analytics over an Iceberg lakehouse, with the lakehouse data generated by dynamic-des.
- [oml-digital-twin-hotrolling](https://github.com/jaehyeon-kim/oml-digital-twin-hotrolling): a streaming digital twin of a steel hot rolling mill, with online machine learning on Kafka and Flink.

## License

MIT
