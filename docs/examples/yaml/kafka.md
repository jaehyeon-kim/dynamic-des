# Kafka Digital Twin (YAML)

This example builds the simulation of the [declarative Kafka example](../declarative/kafka.md) from a YAML blueprint, with no Python. The connectors, the parameters, the task and the telemetry are all declared in the file.

---

## Quick Start

Download the blueprint, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/kafka.yaml
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

The run keeps going until you stop it. When it starts, `KafkaEgress` creates `sim-events` and `sim-telemetry` if they do not exist. The run then publishes task lifecycle events to `sim-events` and lathe metrics to `sim-telemetry`, and applies parameter updates sent to `sim-config`.

The dashboard from the declarative example works with this run unchanged, because it reads the same topics and the same four lathe metrics: `uv run --no-project --with "dynamic-des[kafka]" --with nicegui kafka_dashboard.py`, after downloading [`kafka_dashboard.py`](https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/kafka_dashboard.py).

`KAFKA_BOOTSTRAP_SERVERS` overrides the broker address, because the blueprint reads it with `${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}`.

## Full Source Code

The blueprint wires both Kafka connectors and declares the lathe, the service, the arrival and the task. The finished event of each task is the fixed `payload`, and the `telemetry` entry publishes four built-in statistics of the lathe every 2 simulation seconds. The declarative example also publishes `lathe.avg_wait`, a derived metric, which a blueprint computes only with Python.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/kafka.yaml"
# Kafka digital twin in YAML.
#
# The YAML version of examples/declarative/kafka_example.py. KafkaIngress reads
# parameter updates from sim-config, and KafkaEgress publishes lifecycle events to
# sim-events and lathe metrics to sim-telemetry. KafkaEgress creates its two topics
# when the run starts.
#
# Needs a broker: odctl up kafka-lite. Runs until interrupted with Ctrl + C.
# KAFKA_BOOTSTRAP_SERVERS overrides the broker address.
# Run it with: dynamic-des run examples/yaml/kafka.yaml

simulation:
  sim_id: Line_A
  factor: 1.0
  random_seed: 42

ingress:
  - type: Kafka
    config:
      topic: sim-config
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

egress:
  - type: Kafka
    config:
      event_topic: sim-events
      telemetry_topic: sim-telemetry
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

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
    payload: {path_id: Line_A.service.milling, status: finished}

telemetry:
  # Samples the lathe every 2 simulation seconds.
  - interval: 2.0
    publish:
      lathe.capacity: lathe.capacity
      lathe.in_use: lathe.in_use
      lathe.queue_length: lathe.queue_length
      lathe.utilization: lathe.utilization
```
