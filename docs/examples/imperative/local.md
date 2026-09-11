# Local Simulation (Low-Level Imperative API)

This example demonstrates how to build a dynamic simulation using the low-level **Imperative API** and **Local Connectors**.

Local connectors do not require Docker, Kafka, or any external data stores. They are perfect for testing, benchmarking, or scenarios where parameter changes need to occur at specific wall-clock intervals deterministically.

---

## Quick Start

Download the script, then run it. This example needs no container.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/imperative/local_example.py
```

### With uv

```bash
# 1. Run the imperative simulation
uv run --no-project --with dynamic-des local_example.py
```

### With pip

```bash
# 1. Install the package
pip install dynamic-des

# 2. Run the imperative simulation
python local_example.py
```

## Full Source Code

This script initializes a production line and runs it for 30 simulation seconds. `LocalIngress` schedules two capacity changes: the lathe goes from 1 to 3 at t=10s, then down to 2 at t=20s. Events and telemetry stream directly to your terminal, driven by raw SimPy generators.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

```python title="examples/imperative/local_example.py"
"""Local simulation with scheduled parameter changes, imperative API.

The low-level twin of `declarative/local_example.py`, built on
`DynamicRealtimeEnvironment` directly. It adds what the declarative version does not
have: `LocalIngress` schedules two capacity changes, so the lathe goes from 1 to 3 at
t=10s and down to 2 at t=20s, and the telemetry shows the effect.

Needs no containers. Ends on its own after 30 simulation seconds.
"""

import logging

import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    LocalIngress,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
logger = logging.getLogger("local_example")


def run():
    # 1. Define the system schema
    # Line_A starts with 1 lathe, but has a physical ceiling of 5.
    line_a_params = SimParameter(
        sim_id="Line_A",
        arrival={"standard": DistributionConfig(dist="exponential", rate=1.0)},
        service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
        resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},
    )

    # 2. Setup Environment with Local Connectors
    # Schedule capacity updates: jump to 3 at t=10s, then drop to 2 at t=20s
    ingress = LocalIngress(
        schedule=[
            (10.0, "Line_A.resources.lathe.current_cap", 3),
            (20.0, "Line_A.resources.lathe.current_cap", 2),
        ]
    )
    egress = ConsoleEgress()

    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(line_a_params)
    env.setup_ingress([ingress])
    env.setup_egress([egress])

    # 3. Initialize Resources and Sampler
    res = DynamicResource(env, "Line_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(42))

    # 4. Define Simulation Logic
    def arrival_process(env: DynamicRealtimeEnvironment, res: DynamicResource):
        """Generates tasks based on the dynamic arrival rate."""
        arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
        service_path = "Line_A.service.milling"
        task_id = 0

        while True:
            # Reference-based: arrival_cfg updates automatically via Registry
            yield env.timeout(sampler.sample(arrival_cfg))
            env.process(work_task(env, task_id, res, service_path))
            task_id += 1

    def work_task(
        env: DynamicRealtimeEnvironment,
        task_id: int,
        res: DynamicResource,
        path_id: str,
    ):
        """Models task lifecycle: queued -> started -> finished."""
        task_key = f"task-{task_id}"
        env.publish_event(task_key, {"path_id": path_id, "status": "queued"})

        with res.request() as req:
            yield req
            # Late Binding: Fetch latest config only when work actually starts
            current_service_cfg = env.registry.get_config(path_id)

            env.publish_event(task_key, {"path_id": path_id, "status": "started"})

            yield env.timeout(sampler.sample(current_service_cfg))
            env.publish_event(task_key, {"path_id": path_id, "status": "finished"})

    def telemetry_monitor(env: DynamicRealtimeEnvironment, res: DynamicResource):
        """Streams system health metrics every 2 seconds."""
        while True:
            env.publish_telemetry("Line_A.resources.lathe.capacity", res.capacity)
            env.publish_telemetry("Line_A.resources.lathe.in_use", res.in_use)
            env.publish_telemetry(
                "Line_A.resources.lathe.queue_length", len(res.queue.items)
            )

            util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
            env.publish_telemetry("Line_A.resources.lathe.utilization", util)
            yield env.timeout(2.0)

    # 5. Run the Simulation
    env.process(arrival_process(env, res))
    env.process(telemetry_monitor(env, res))

    logging.info("Simulation started. Watch capacity change at t=10.0s and 20.0s...")
    try:
        env.run(until=30)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
```
