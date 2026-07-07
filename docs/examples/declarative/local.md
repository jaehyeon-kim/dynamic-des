# Local Simulation (Standard Declarative API)

This example demonstrates how to build a dynamic simulation using the declarative **Standard API (`SimulationContext`)** and **Local Connectors**.

Local connectors do not require Docker, Kafka, or any external data stores. They are perfect for testing, benchmarking, or scenarios where parameter changes need to occur at specific wall-clock intervals deterministically.

---

## Code

This script initializes a production line, schedules a capacity update to happen 10 seconds into the future, and streams telemetry directly to your terminal.

```python
import logging
from dynamic_des import SimulationContext, ConsoleEgress, LocalIngress

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s")

def run():
    # 1. Initialize SimulationContext (Builder Pattern)
    # Schedule capacity to jump to 3 at t=10s, then drop to 2 at t=20s
    app = (
        SimulationContext(sim_id="Line_A", factor=1.0, random_seed=42)
        .add_resource("lathe", current_cap=1, max_cap=5)
        .add_arrival("standard", dist="exponential", rate=1.0)
        .add_service("milling", dist="normal", mean=3.0, std=0.5)
        .add_ingress(LocalIngress(
            schedule=[
                (10.0, "Line_A.resources.lathe.current_cap", 3),
                (20.0, "Line_A.resources.lathe.current_cap", 2),
            ]
        ))
        .add_egress(ConsoleEgress())
    )

    # 2. Define Simulation Processes using Decorators
    @app.arrival_loop("standard")
    def arrival_process(context: SimulationContext):
        task_id = 0
        while True:
            yield context.wait_for_arrival("standard")
            context.spawn(work_task(task_id))
            task_id += 1

    @app.task(service_id="milling", resource_id="lathe")
    def work_task(task_id: int):
        return {"part_id": task_id}

    @app.telemetry_loop(interval=2.0)
    def telemetry_monitor(context: SimulationContext):
        res = context.get_resource("lathe")
        context.env.publish_telemetry("Line_A.lathe.capacity", res.capacity)
        context.env.publish_telemetry("Line_A.lathe.in_use", res.in_use)
        context.env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))

    # 3. Run the Simulation
    print("Simulation started. Watch capacity change at t=10s and t=20s...")
    app.run(until=25.0)

if __name__ == "__main__":
    run()
```
