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

The examples live in the [`examples/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) folder of the repository, not in the installed package, so clone it first. `examples/README.md` lists what each one needs.

```bash
git clone https://github.com/jaehyeon-kim/dynamic-des.git
cd dynamic-des
uv sync --all-extras
```

Dynamic DES comes with built-in examples so you can see it in action immediately. You do not need to write a single line of code to test this out.

**1. Run the local, dependency-free simulation:**

```bash
uv run examples/declarative/local_example.py
```

**2. Run the full Real-Time Digital Twin stack with Kafka and a live UI:**

```bash
# Start the background Kafka cluster (requires Docker)
odctl up kafka-lite

# Open a new terminal and run the simulation
# Ctrl + C to stop
uv run examples/declarative/kafka_example.py

# Open a new terminal and start the monitoring dashboard. It needs nicegui, which
# is not a dependency of the library, so --with supplies it for this run only.
# It serves http://localhost:8080 rather than opening a browser. Ctrl + C to stop.
uv run --with nicegui examples/kafka_dashboard.py

# Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

**3. Backfill history, then go live, in one run:**

```bash
odctl up kafka-lite

# Ten minutes of backdated history go to Parquet, generated instantly rather than
# waited for, then the run switches to real time and the live tail goes to Kafka
# for sixty seconds. It takes about a minute in total, nearly all of it the live half.
uv run examples/declarative/backfill_live_example.py

odctl down kafka-lite --volumes
```

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
