# Local Simulation

This example demonstrates how to build a dynamic simulation using **Local Connectors**.

Local connectors do not require Docker, Kafka, or any external data stores. They are perfect for testing, benchmarking, or scenarios where parameter changes need to occur at specific wall-clock intervals deterministically.

## Execution

Local connectors are dependency-free and do not require Docker or Kafka. You can run the built-in demo directly from your terminal:

```bash
# Core library is enough for local examples
pip install dynamic-des

# Run the example
ddes-local-example
```

## Code

This script initializes a production line, schedules a capacity update to happen 10 seconds into the future, and streams telemetry directly to your terminal.

```python
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

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s")

def run():
    # 1. Define the initial system schema
    line_a_params = SimParameter(
        sim_id="Line_A",
        arrival={"standard": DistributionConfig(dist="exponential", rate=1.0)},
        service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
        resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},
    )

    # 2. Setup Environment with Local Connectors
    # Schedule capacity to jump to 3 at t=10s, then drop to 2 at t=20s
    ingress = LocalIngress(
        schedule=[
            (10.0, "Line_A.resources.lathe.current_cap", 3),
            (20.0, "Line_A.resources.lathe.current_cap", 2),
        ]
    )
    # Egress simply prints to the console
    egress = ConsoleEgress()

    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(line_a_params)
    env.setup_ingress([ingress])
    env.setup_egress([egress])

    # 3. Initialize Resources and Sampler
    res = DynamicResource(env, "Line_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(42))

    # 4. Define Simulation Logic
    def arrival_process(env, res):
        arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(arrival_cfg))
            env.process(work_task(env, task_id, res, "Line_A.service.milling"))
            task_id += 1

    def work_task(env, task_id, res, path_id):
        task_key = f"task-{task_id}"
        env.publish_event(task_key, {"path_id": path_id, "status": "queued"})

        with res.request() as req:
            yield req
            current_service_cfg = env.registry.get_config(path_id)
            env.publish_event(task_key, {"path_id": path_id, "status": "started"})
            yield env.timeout(sampler.sample(current_service_cfg))
            env.publish_event(task_key, {"path_id": path_id, "status": "finished"})

    def telemetry_monitor(env, res):
        """Streams system health metrics every 2 seconds."""
        while True:
            env.publish_telemetry("Line_A.lathe.capacity", res.capacity)
            env.publish_telemetry("Line_A.lathe.in_use", res.in_use)
            env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))
            yield env.timeout(2.0)

    # 5. Run the Simulation
    env.process(arrival_process(env, res))
    env.process(telemetry_monitor(env, res))

    print("Simulation started. Watch capacity change at t=10.0s and 20.0s...")
    try:
        env.run(until=30)
    finally:
        env.teardown()

if __name__ == "__main__":
    run()
```
