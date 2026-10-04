# In-Memory Store (Redis, YAML)

This example builds the simulation of the [declarative Redis example](../declarative/redis.md) from a YAML blueprint, with no Python. `RedisEgress` writes part records to the `part_events` stream, which each record's `__stream__` key names, and `RedisIngress` subscribes to the `simulation_params` channel.

---

## Quick Start

Download the blueprint, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/redis.yaml
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Start the Valkey database
odctl up valkey

# 3. Run the blueprint (Ctrl + C to stop)
uv run --no-project --with "dynamic-des[redis]" ddes run redis.yaml

# 4. Clean up the infrastructure when finished
odctl down valkey --volumes
```

### With pip

```bash
# 1. Install the package with the redis extra, and odctl for the containers
pip install "dynamic-des[redis]" "odctl>=0.5.1"

# 2. Start the Valkey database
odctl up valkey

# 3. Run the blueprint (Ctrl + C to stop)
ddes run redis.yaml

# 4. Clean up the infrastructure when finished
odctl down valkey --volumes
```

## What It Does

The run writes part events to the `part_events` stream, and the lag telemetry to `events`, until you stop it. In a second terminal, raise the arrival rate while it runs:

```bash
docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
```

`RedisIngress` does not log the message it receives, so the sign that the update landed is that `XLEN part_events` climbs about five times faster.

## Full Source Code

`record_part` has no service or resource, so it publishes its fixed `payload` as soon as an arrival spawns it, with the task id added as `part_id`. The declarative example draws a random part type for every arrival. A mapping payload is a constant, so every part here has type `A`.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/redis.yaml"
# Redis Streams output with live parameter updates, in YAML.
#
# The YAML version of examples/declarative/redis_example.py. Each part_arrival
# spawns record_part, a task with no service or resource, which publishes its
# payload at once. RedisEgress writes it to the part_events stream that the
# __stream__ key names, while RedisIngress subscribes to the simulation_params
# channel.
#
# Needs Valkey: odctl up valkey. Runs until interrupted with Ctrl + C.
# Run it with: ddes run examples/yaml/redis.yaml

simulation:
  sim_id: Factory
  factor: 1.0

ingress:
  - type: Redis
    config:
      # The odctl valkey profile disables the unauthenticated default user, so the
      # URL carries credentials.
      url: redis://user:password@localhost:6379/0
      channel_name: simulation_params

egress:
  - type: Redis
    config:
      url: redis://user:password@localhost:6379/0
      stream_name: events

arrivals:
  part_arrival: {dist: exponential, rate: 2.0, spawn: record_part}

tasks:
  record_part:
    # id_field adds the task id as part_id.
    payload: {__stream__: part_events, type: A, status: arrived}
    id_field: part_id
```
