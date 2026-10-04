# Backfill Then Go Live (YAML)

This example builds the simulation of [`examples/declarative/backfill_live_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/backfill_live_example.py) from a YAML blueprint, with no Python. Until `go_live_at` the run is unpaced and writes backdated history to Parquet. From `go_live_at` it is paced in real time and publishes to Kafka. See [Backfill Then Go Live in One Run](../../guides/backfill-then-live.md) for how the two instants work.

---

## Quick Start

Download the blueprint, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/backfill_live.yaml
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Start the Kafka broker and schema registry
odctl up kafka-lite

# 3. Run the blueprint
uv run --no-project --with "dynamic-des[kafka,parquet]" ddes run backfill_live.yaml

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
ddes run backfill_live.yaml

# 4. Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

## What It Does

Ten minutes of history are written to `data/backfill/` within the first second, then the run publishes to `sim-events` and `sim-telemetry` in real time for 60 seconds and ends. The history folder is created on the first write, and `KafkaEgress` creates the two topics when it starts.

## Full Source Code

`logical_start_time: -10m` and `go_live_at: now` are read against one moment, when the file is loaded, so they are exactly ten minutes apart, and `until: 11m` adds one minute of live tail. `when: history` sends the records stamped before `go_live_at` to Parquet, and `when: live` sends the rest to Kafka.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/backfill_live.yaml"
# Backfill then go live in one run, in YAML.
#
# The YAML version of examples/declarative/backfill_live_example.py. Until
# go_live_at the clock is detached from the wall clock, so ten minutes of backdated
# history are written to Parquet as fast as the machine allows. From go_live_at the
# run is paced at one simulated second per real second, and the same events go to
# Kafka for one minute.
#
# Needs a broker: odctl up kafka-lite.
# Run it with: ddes run examples/yaml/backfill_live.yaml

simulation:
  sim_id: Line_A
  # Unpaced to begin with, so the history costs no real time.
  factor: 0.0
  random_seed: 42
  logical_start_time: -10m
  # From here the same run is paced at one simulated second per real second.
  go_live_at: now

egress:
  - type: Parquet
    config:
      default_path: data/backfill/events.parquet
    # Records stamped before go_live_at.
    when: history
  - type: Kafka
    config:
      event_topic: sim-events
      telemetry_topic: sim-telemetry
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
    # Records stamped at or after go_live_at.
    when: live

# Only batch_size governs the history. The interval flush starts at go-live, so the
# live tail also flushes every 10 seconds.
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
  # Ten minutes of history, then one minute of live tail.
  until: 11m
```
