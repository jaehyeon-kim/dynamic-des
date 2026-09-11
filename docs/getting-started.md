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

# For all backends (Kafka, Redis, Postgres, Dashboard, Avro)
pip install "dynamic-des[all]"
```

---

## Example Infrastructure

Every example that needs a broker, a database or an object store gets it from [odctl](https://github.com/jaehyeon-kim/odctl), a separate CLI that manages curated Docker Compose stacks. Install it once:

```bash
# With uv
uv tool install odctl

# Or with pip
pip install odctl
```

`odctl list -d` shows every profile and the ports it publishes. The examples here use four of them: `kafka-lite`, `postgres`, `valkey` and `storage`.

### Breaking change

Dynamic DES used to ship its own `docker-compose.yml` and eight console scripts that drove it: `ddes-kafka-infra-up`, `ddes-kafka-infra-down`, `ddes-storage-infra-up`, `ddes-storage-infra-down`, `ddes-postgres-infra-up`, `ddes-postgres-infra-down`, `ddes-redis-infra-up` and `ddes-redis-infra-down`. All eight have been removed. Replace `ddes-<name>-infra-up` with `odctl up <profile>` and `ddes-<name>-infra-down` with `odctl down <profile> --volumes`, using `kafka-lite` for kafka, `valkey` for redis, and the same name for `postgres` and `storage`.

Three endpoints moved with the switch. The Postgres database is now `odctl` rather than `ddes`. The object store bucket is `odctl-dev` rather than `des-dev`. Valkey now requires the `user` / `password` credentials, so the connection URL is `redis://user:password@localhost:6379/0`.

Valkey also needs one extra grant before `RedisIngress` works. odctl creates the `user` account with `~* +@all`, which covers keys and commands but not Pub/Sub channels, so a subscribe is refused with NOPERM. Run this once after `odctl up valkey`:

```bash
docker exec -it valkey valkey-cli --user user --pass password ACL SETUSER user allchannels
```

---

## Quick Start: Zero-Setup Demos

Dynamic DES comes with built-in examples so you can see it in action immediately. You do not need to write a single line of code to test this out.

**1. Run the local, dependency-free simulation:**

```bash
ddes-local
```

**2. Run the full Real-Time Digital Twin stack with Kafka and a live UI:**

```bash
# Start the background Kafka cluster (requires Docker)
odctl up kafka-lite

# Open a new terminal and run the simulation
# Ctrl + C to stop
ddes-kafka

# Open a new terminal and start the monitoring dashboard (opens in browser)
# Visit http://localhost:8080
# Ctrl + C to stop
ddes-kafka-dashboard

# Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

The control dashboard lets you update simulation parameters live and watch the telemetry react without restarting the run:

<div align="center">
  <img src="assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="800" />
</div>

## Build Your Own

Ready to build your own system? We have prepared a gallery of real-world use cases to demonstrate how to architect your simulation.

- [Local Simulation](examples/declarative/local.md): A dependency-free approach to testing.
- [Kafka Digital Twin](examples/declarative/kafka.md): A full manufacturing architecture with dynamic queues.
- [Fast-Forward to Data Lake](examples/declarative/history.md): Batch processing simulation data into Parquet.
