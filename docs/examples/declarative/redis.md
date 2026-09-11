# In-Memory Store (Redis) Declarative API

This example demonstrates how to integrate `dynamic-des` with a high-performance Redis cache using the declarative **Standard API (`SimulationContext`)**.

By combining `RedisIngress` and `RedisEgress`, your simulation can achieve sub-millisecond latency for both reading dynamic parameters via Pub/Sub and writing high-throughput telemetry data via Redis Streams.

---

## 1. Streaming to Redis

When generating events, `RedisEgress` streams outputs directly into a Redis Stream with `XADD`. The stream is the one named in the constructor, which this example sets to `events`.

```python
app = (
    SimulationContext(sim_id="Factory", factor=1.0)
    .add_egress(RedisEgress(REDIS_URL, stream_name="events"))
)

# ... inside the generator
part_event = {
    "__stream__": "part_events",
    "part_id": 1,
    "status": "arrived",
}
context.publish("factory_event", part_event)
```

Each entry holds one field, `payload`, containing the JSON of the published record. `RedisEgress` reads `__stream__` from inside `value`, which is where `publish_event` puts the dictionary you pass it, so part records go to `part_events` and everything else to the `events` stream named in the constructor. The key itself is removed before writing, so it does not appear in the stored payload.

## 2. Dynamic Parameter Updates (Ingress)

This example attaches a `RedisIngress` listening to a Pub/Sub channel called `simulation_params`. While the simulation is running, you can dynamically update parameters (like speeding up the arrival rate) by simply publishing a JSON string to the channel!

```python
app.add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
```

## 3. Quick Start

Download the script, then run it. The run keeps generating parts until you stop it with Ctrl + C. **To test the dynamic ingress updates**, open a second terminal while the simulation is running and execute the `PUBLISH` command below.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/redis_example.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Spin up the Valkey database
odctl up valkey

# 3. Run the declarative simulation
uv run --no-project --with "dynamic-des[redis]" redis_example.py
```

### With pip

```bash
# 1. Install the package with the redis extra, and odctl for the containers
pip install "dynamic-des[redis]" "odctl>=0.5.1"

# 2. Spin up the Valkey database
odctl up valkey

# 3. Run the declarative simulation
python redis_example.py
```

**In a second terminal, execute the dynamic parameter update:**
```bash
# Connect to the Valkey container and publish the parameter update
docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
```
`RedisIngress` does not log the message it receives, so the sign that the update landed is the throughput. The arrival rate goes from 2.0 to 10.0 per second, and `XLEN events` climbs roughly three to four times faster than before. It is not the full factor of five because the same stream also carries simulation lag telemetry at a steady rate.

```bash
# Clean up the infrastructure when finished
odctl down valkey --volumes
```

---

## Full Source Code

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

```python title="examples/declarative/redis_example.py"
"""Redis Streams output with live parameter updates, declarative API.

`Factory` writes every record to the `events` Redis Stream through `RedisEgress`, while
`RedisIngress` subscribes to the `simulation_params` channel, so publishing a message to
that channel changes the arrival rate of a running simulation.

Needs Valkey: `odctl up valkey`. Runs until interrupted with Ctrl + C.
"""

import logging
import random
from datetime import datetime

from dynamic_des import RedisEgress, RedisIngress, SimulationContext

logger = logging.getLogger(__name__)

# The odctl `valkey` profile disables the unauthenticated default user, so the
# URL carries credentials. A plain Redis without auth takes redis://localhost:6379/0.
REDIS_URL = "redis://user:password@localhost:6379/0"

app = (
    SimulationContext(sim_id="Factory", factor=1.0)
    .add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
    .add_egress(RedisEgress(REDIS_URL, stream_name="events"))
    .add_arrival("part_arrival", dist="exponential", rate=2.0)
)


@app.arrival_loop("part_arrival")
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


def run():
    logger.info("Starting Declarative Redis Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running: docker exec -it valkey valkey-cli --user user "
        "--pass password PUBLISH simulation_params "
        '\'{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}\''
    )
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
```
