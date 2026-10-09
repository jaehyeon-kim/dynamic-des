# Getting Started

This page installs Dynamic DES, installs odctl to run the containers the examples need, and runs the first examples.

## Installation

Install the core library:

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

---

## Example Infrastructure

Every example that needs a broker, a database or an object store gets it from [odctl](https://github.com/jaehyeon-kim/odctl), a separate CLI that manages curated Docker Compose stacks. Install it once:

```bash
# With uv
uv tool install "odctl>=1.0,<2"

# Or with pip
pip install "odctl>=1.0,<2"
```

`odctl list -d` shows every profile and the ports it publishes. The examples here use five of them: `kafka-lite`, `postgres`, `valkey`, `storage` and `catalog`.

### Starting infrastructure

Each example needs one odctl profile, started before you run it and torn down after:

| Example | Start | Stop |
|---|---|---|
| local | nothing needed | |
| kafka, backfill-live, dashboard | `odctl up kafka-lite` | `odctl down kafka-lite --volumes` |
| postgres | `odctl up postgres` | `odctl down postgres --volumes` |
| redis | `odctl up valkey` | `odctl down valkey --volumes` |
| parquet with `USE_S3=true` | `odctl up storage` | `odctl down storage --volumes` |
| iceberg | `odctl up catalog` | `odctl down catalog --volumes` |

The YAML blueprints in `examples/yaml/` need the same profile as their declarative twin.

Kafka and Redis are the two whose profile names are not what you would guess, because odctl ships a one-broker Kafka as `kafka-lite` and uses Valkey rather than Redis.

### Endpoints these profiles publish

The Postgres database is `odctl`. The object store bucket is `odctl-dev`. Valkey requires the `user` / `password` credentials, so the connection URL is `redis://user:password@localhost:6379/0`.

---

## Quick Start: Running an Example

Dynamic DES ships runnable examples in the [`examples/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) folder of the repository. Download the ones you want, then run them.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/local_example.py
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/kafka_example.py
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/kafka_dashboard.py
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/backfill_live_example.py
```

With [uv](https://docs.astral.sh/uv/), and odctl installed as above:

```bash
# 1. Local, dependency-free simulation
uv run --no-project --with dynamic-des local_example.py

# 2. Start the Kafka broker and schema registry (requires Docker)
odctl up kafka-lite

# 3. Run the real-time digital twin (Ctrl + C to stop)
uv run --no-project --with "dynamic-des[kafka]" kafka_example.py

# 4. In a second terminal, watch and steer the run from the dashboard. It serves
#    http://localhost:8080 rather than opening a browser. Ctrl + C to stop.
uv run --no-project --with "dynamic-des[kafka]" --with nicegui kafka_dashboard.py

# 5. Backfill ten minutes of history to Parquet, generated instantly rather than
#    waited for, then tail live to Kafka for sixty seconds
uv run --no-project --with "dynamic-des[kafka,parquet]" backfill_live_example.py

# 6. Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

With pip, install `"dynamic-des[kafka,parquet]" nicegui` once and run each file with `python`. Any other example needs the profile listed in [Starting infrastructure](#starting-infrastructure), and the [Backfill Then Go Live](guides/backfill-then-live.md) guide explains step 5.

The control dashboard from step 4 changes simulation parameters while the run is going, and the telemetry reacts without a restart:

<div align="center">
  <img src="../assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="800" />
</div>

### A YAML blueprint

The same local simulation is also a YAML file, run with the `ddes` command that the package installs. It needs no Python:

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/local.yaml

# With uv
uv run --no-project --with dynamic-des ddes run local.yaml

# Or with pip, after `pip install dynamic-des`
ddes run local.yaml
```

Every declarative example has a YAML twin, in the YAML tab of its [example page](examples/local.md). [YAML Blueprints, from First File to Connectors](guides/yaml-blueprints.md) shows how to write one.

## Build Your Own

Each example is written three ways: with the low-level API, with the declarative API and as a YAML blueprint. Its page shows the versions in tabs. Backfill Then Go Live has no low-level version, and the advanced orders example exists only as YAML.

| Example | What it shows | Low-level | Declarative | YAML |
|---|---|---|---|---|
| [Local Simulation](examples/local.md) | prints events and telemetry, with no container | [`local_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/imperative/local_example.py) | [`local_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/local_example.py) | [`local.yaml`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/yaml/local.yaml) |
| [Kafka Digital Twin](examples/kafka.md) | takes updates from Kafka and publishes events and telemetry to Kafka | [`kafka_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/imperative/kafka_example.py) | [`kafka_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/kafka_example.py) | [`kafka.yaml`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/yaml/kafka.yaml) |
| [Fast-Forward to Parquet](examples/parquet.md) | a week generated at `factor=0.0`, written to Parquet | [`parquet_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/imperative/parquet_example.py) | [`parquet_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/parquet_example.py) | [`parquet.yaml`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/yaml/parquet.yaml) |
| [Fast-Forward to Iceberg](examples/iceberg.md) | a day generated at `factor=0.0`, appended to an Iceberg table | [`iceberg_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/imperative/iceberg_example.py) | [`iceberg_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/iceberg_example.py) | [`iceberg.yaml`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/yaml/iceberg.yaml) |
| [Relational DB (Postgres)](examples/postgres.md) | writes orders to PostgreSQL and takes updates from a table | [`postgres_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/imperative/postgres_example.py) | [`postgres_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/postgres_example.py) | [`postgres.yaml`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/yaml/postgres.yaml) |
| [In-Memory Store (Redis)](examples/redis.md) | writes to a Redis Stream and takes updates from Pub/Sub | [`redis_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/imperative/redis_example.py) | [`redis_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/redis_example.py) | [`redis.yaml`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/yaml/redis.yaml) |
| [Backfill Then Go Live](examples/backfill-live.md) | backdated history to Parquet, then live to Kafka | no | [`backfill_live_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/backfill_live_example.py) | [`backfill_live.yaml`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/yaml/backfill_live.yaml) |
| [Orders with Line Items (Advanced YAML)](examples/advanced-postgres-orders.md) | orders with line items, from a blueprint with one Python function | no | no | [`postgres_orders.yaml`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/yaml/advanced/postgres_orders.yaml) |
