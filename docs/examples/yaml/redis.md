# In-Memory Store (Redis, YAML)

This example builds the same simulation as the [declarative Redis example](../declarative/redis.md) from a YAML blueprint. `RedisEgress` writes every record to the `events` stream, and `RedisIngress` subscribes to the `simulation_params` channel. The part generator stays in `redis_logic.py` as a process.

---

## Quick Start

Download the blueprint and the Python module beside it into the same folder, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/redis.yaml
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/redis_logic.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Start the Valkey database
odctl up valkey

# 3. Run the blueprint (Ctrl + C to stop)
uv run --no-project --with "dynamic-des[redis]" dynamic-des run redis.yaml

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
dynamic-des run redis.yaml

# 4. Clean up the infrastructure when finished
odctl down valkey --volumes
```

## What It Does

The run writes part events to the `events` stream until you stop it. In a second terminal, raise the arrival rate while it runs:

```bash
docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
```

`RedisIngress` does not log the message it receives, so the sign that the update landed is that `XLEN events` climbs faster.

## Full Source Code

The part generator is listed under `processes`, because it draws a random part type for every arrival, which YAML has no way to express.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/redis.yaml"
# Redis Streams output with live parameter updates, in YAML.
#
# The twin of examples/declarative/redis_example.py. Factory writes every record to
# the events Redis Stream through RedisEgress, while RedisIngress subscribes to the
# simulation_params channel. The part generator stays in redis_logic.py beside
# this file.
#
# Needs Valkey: odctl up valkey. Runs until interrupted with Ctrl + C.
# Run it with: dynamic-des run examples/yaml/redis.yaml

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
  part_arrival: {dist: exponential, rate: 2.0}

processes:
  # Publishes one part event on every part_arrival.
  - !python redis_logic.part_generator
```

```python title="examples/yaml/redis_logic.py"
"""Python for examples/yaml/redis.yaml: the part generator."""

import random
from datetime import datetime


def part_generator(context):
    part_id = 1
    while True:
        yield context.wait_for_arrival("part_arrival")

        part_event = {
            "__stream__": "part_events",
            "part_id": part_id,
            "type": random.choice(["A", "B", "C"]),
            "timestamp": datetime.utcnow().isoformat(),
            "status": "arrived",
        }

        context.publish("factory_event", part_event)
        part_id += 1
```
