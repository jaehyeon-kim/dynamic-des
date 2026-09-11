# Getting Started

Ready to build real-time digital twins? This guide will walk you through installing Dynamic DES, running the built-in zero-setup demos, and exploring the core infrastructure.

## Installation

Install the core library:

```bash
pip install dynamic-des
```

To include specific backends:

```bash
# For Kafka support
pip install "dynamic-des[kafka]"

# For Confluent Schema Registry (Avro)
pip install "dynamic-des[kafka,confluent]"

# For AWS Glue Schema Registry (Avro)
pip install "dynamic-des[kafka,glue]"

# For Parquet support (required for data lake integration)
pip install "dynamic-des[parquet]"

# For all backends (Kafka, Redis, Postgres, Avro, Parquet)
pip install "dynamic-des[all]"
```

---

## Example Infrastructure

Every example that needs a broker, a database or an object store gets it from [odctl](https://github.com/jaehyeon-kim/odctl), a separate CLI that manages curated Docker Compose stacks. Install it once:

```bash
# With uv
uv tool install "odctl>=0.5.1"

# Or with pip
pip install "odctl>=0.5.1"
```

`odctl list -d` shows every profile and the ports it publishes. The examples here use four of them: `kafka-lite`, `postgres`, `valkey` and `storage`.

### Starting infrastructure

Each example needs one odctl profile, started before you run it and torn down after:

| Example | Start | Stop |
|---|---|---|
| local | nothing needed | |
| kafka, backfill-live, dashboard | `odctl up kafka-lite` | `odctl down kafka-lite --volumes` |
| postgres | `odctl up postgres` | `odctl down postgres --volumes` |
| redis | `odctl up valkey` | `odctl down valkey --volumes` |
| history with `USE_S3=true` | `odctl up storage` | `odctl down storage --volumes` |

Kafka and Redis are the two whose profile names are not what you would guess, because odctl ships a one-broker Kafka as `kafka-lite` and uses Valkey rather than Redis.

### What changed

Dynamic DES used to ship its own `docker-compose.yml` and a set of `ddes-*` console scripts. Both are gone. odctl provides the containers, and the examples live in `examples/` at the repository root, run directly with `uv run`.

Three endpoints moved with the switch. The Postgres database is now `odctl` rather than `ddes`. The object store bucket is `odctl-dev` rather than `des-dev`. Valkey now requires the `user` / `password` credentials, so the connection URL is `redis://user:password@localhost:6379/0`.

---

## Quick Start: Running an Example

Dynamic DES ships runnable examples in the [`examples/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) folder of the repository. Download the ones you want, then run them.

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

Guide: [Backfill then live](guides/backfill-then-live.md).

The control dashboard lets you update simulation parameters live and watch the telemetry react without restarting the run:

<div align="center">
  <img src="assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="800" />
</div>

## Build Your Own

Ready to build your own system? We have prepared a gallery of real-world use cases to demonstrate how to architect your simulation.

- [Local Simulation](examples/declarative/local.md): A dependency-free approach to testing.
- [Kafka Digital Twin](examples/declarative/kafka.md): A full manufacturing architecture with dynamic queues.
- [Fast-Forward to Data Lake](examples/declarative/history.md): Batch processing simulation data into Parquet.
