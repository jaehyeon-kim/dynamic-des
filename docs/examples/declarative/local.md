# Local Simulation (Standard Declarative API)

This example demonstrates how to build a dynamic simulation using the declarative **Standard API (`SimulationContext`)** and **Local Connectors**.

Local connectors do not require Docker, Kafka, or any external data stores. They are perfect for testing and benchmarking. This example adds `ConsoleEgress` and no ingress, so the lathe keeps the capacity it starts with for the whole run. Its imperative twin shows how `LocalIngress` schedules parameter changes at set times.

---

## Quick Start

The examples are in the repository, not in the installed package, so clone it first.

```bash
git clone https://github.com/jaehyeon-kim/dynamic-des.git
cd dynamic-des
uv sync
```

Or with pip:

```bash
pip install dynamic-des
```

This example needs no container.

```bash
# Run the declarative simulation (no infrastructure required)
uv run examples/declarative/local_example.py
```

## Full Source Code

This script initializes a production line, runs it for 60 simulation seconds, and streams events and telemetry directly to your terminal.

```python title="examples/declarative/local_example.py"
"""Local simulation, declarative API, no containers.

The smallest complete example. `Factory_A` is built with `SimulationContext` and writes
to `ConsoleEgress`, so events and telemetry are printed to the terminal and nothing
external is involved.

Start here. It needs no broker, no database and no object store, and it ends on its own
after 60 simulation seconds.
"""

import logging

from dynamic_des import ConsoleEgress, SimulationContext

# Logging is configured here rather than in a wrapper, because this script is run
# directly. Without it the run produces no output at all.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

# ==========================================
# 1. Declarative Infrastructure Builder
# ==========================================
app = (
    SimulationContext(sim_id="Factory_A", factor=1.0)
    .add_egress(ConsoleEgress())
    .add_resource("lathe", current_cap=2, max_cap=5)
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    .add_arrival("standard", dist="exponential", rate=1.0)
)


# ==========================================
# 2. Simulation Logic (Decorators)
# ==========================================
@app.task(service_id="milling", resource_id="lathe")
def process_part(task_id: int):
    """Executes the milling service and returns the custom payload."""
    return {"event_type": "part_produced", "part_id": task_id, "quality": "A"}


@app.arrival_loop("standard")
def arrival_generator(context):
    """Continuously spawns new parts based on the 'standard' arrival distribution."""
    task_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(process_part(task_id))
        task_id += 1


@app.telemetry_loop(interval=2.0)
def telemetry_generator(context):
    """Samples the hidden state of the resources every 2 simulation seconds."""
    res = context.get_resource("lathe")
    util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0

    context.publish("utilization", util)
    context.publish("queue_length", len(res.queue.items))


# ==========================================
# 3. Execution
# ==========================================
def run():
    """Starts the local simulation for a fixed duration."""
    logger.info("Starting Declarative Local Example. Running for 60 seconds...")
    app.run(until=60)


if __name__ == "__main__":
    run()
```
