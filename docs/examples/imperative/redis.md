# In-Memory Store (Redis) Imperative API

This example demonstrates how to integrate `dynamic-des` with a high-performance Redis cache using the low-level **Imperative API (`DynamicRealtimeEnvironment`)**.

This is useful if you are migrating existing SimPy generators and prefer to handle `env.process()` and component registration manually rather than using the Builder Pattern.

---

## 1. Quick Start

The examples are in the repository, not in the installed package, so clone it first.

```bash
git clone https://github.com/jaehyeon-kim/dynamic-des.git
cd dynamic-des
uv sync --extra redis
uv tool install "odctl>=0.5.1"   # containers for the examples
```

Or with pip:

```bash
pip install "dynamic-des[redis]"
pip install "odctl>=0.5.1"
```

Run the script directly with `uv run`. It keeps generating parts until you stop it with Ctrl + C. Every entry is written to the `events` Redis Stream, which is the name passed to `RedisEgress`.

```bash
# 1. Spin up the Valkey database with odctl
odctl up valkey

# 2. Run the imperative simulation
uv run examples/imperative/redis_example.py
```

**In a second terminal, execute the dynamic parameter update:**
```bash
# Connect to the Valkey container and publish the parameter update
docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
```

`RedisIngress` does not log the message it receives, so the sign that the update landed is the throughput. The arrival rate goes from 2.0 to 10.0 per second, and `XLEN events` climbs roughly three to four times faster than before. It is not the full factor of five because the same stream also carries simulation lag telemetry at a steady rate.

```bash
# 3. Clean up the infrastructure when finished
odctl down valkey --volumes
```

---

## Full Source Code

```python title="examples/imperative/redis_example.py"
"""Redis Streams output with live parameter updates, imperative API.

The low-level twin of `declarative/redis_example.py`, wiring the environment and
connectors by hand. `RedisEgress` writes to the `events` stream and `RedisIngress`
subscribes to the `simulation_params` channel.

Needs Valkey: `odctl up valkey`. Runs until interrupted with Ctrl + C.
"""

import logging
import random
from datetime import datetime

import numpy as np

from dynamic_des import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    RedisEgress,
    RedisIngress,
    Sampler,
    SimParameter,
)

logger = logging.getLogger("redis_example")

# The odctl `valkey` profile disables the unauthenticated default user, so the
# URL carries credentials. A plain Redis without auth takes redis://localhost:6379/0.
REDIS_URL = "redis://user:password@localhost:6379/0"


def run():
    params = SimParameter(
        sim_id="Factory",
        arrival={"part_arrival": DistributionConfig(dist="exponential", rate=2.0)},
    )

    # Attach egress instance
    egress = RedisEgress(REDIS_URL, stream_name="events")

    # Attach ingress for dynamic parameter updates
    ingress = RedisIngress(REDIS_URL, channel_name="simulation_params")

    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(params)
    env.setup_egress([egress])
    env.setup_ingress([ingress])

    sampler = Sampler(rng=np.random.default_rng(42))

    def part_process(env: DynamicRealtimeEnvironment):
        arrival_cfg = env.registry.get_config("Factory.arrival.part_arrival")
        part_id = 1

        while True:
            yield env.timeout(sampler.sample(arrival_cfg))

            part_event = {
                "__stream__": "part_events",
                "part_id": part_id,
                "type": random.choice(["A", "B", "C"]),
                "timestamp": datetime.utcnow().isoformat(),
                "status": "arrived",
            }
            env.publish_event(f"part-{part_id}", part_event)

            part_id += 1

    env.process(part_process(env))

    logger.info("Starting Imperative Redis Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running: docker exec -it valkey valkey-cli --user user "
        "--pass password PUBLISH simulation_params "
        '\'{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}\''
    )

    try:
        env.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
```
