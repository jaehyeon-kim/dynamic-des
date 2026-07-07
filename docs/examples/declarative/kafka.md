# Kafka Digital Twin (Standard Declarative API)

This example demonstrates how to integrate `dynamic-des` into a full event-driven architecture using the declarative **Standard API (`SimulationContext`)**.

By replacing the local connectors with `KafkaIngress` and `KafkaEgress`, the simulation becomes a fully detached microservice. It listens for external JSON commands to mutate its state, and streams telemetry and strictly-typed Pydantic events to outbound topics.

---

## Code

This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

```python
import logging
import time
from pydantic import BaseModel
from dynamic_des import SimulationContext, KafkaAdminConnector, KafkaEgress, KafkaIngress

logging.basicConfig(level=logging.INFO)

# 1. Define Strongly-Typed Event Payloads
class TaskEvent(BaseModel):
    """
    Thanks to duck-typing, we can pass this Pydantic model directly into
    our event-decorated tasks. The KafkaEgress layer handles the extraction!
    """
    path_id: str
    status: str

def run():
    BOOTSTRAP_SERVERS = "localhost:9092"
    sim_id = "Line_A"

    # 2. Bootstrap Kafka Topics
    admin_connector = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS)
    admin_connector.create_topics([
        {"name": "sim-config"}, {"name": "sim-telemetry"}, {"name": "sim-events"}
    ])
    time.sleep(2)

    # 3. Setup Environment with Kafka Connectors
    app = (
        SimulationContext(sim_id=sim_id, factor=1.0, random_seed=42)
        .add_resource("lathe", current_cap=1, max_cap=10)
        .add_arrival("standard", dist="exponential", rate=1.0)
        .add_service("milling", dist="normal", mean=3.0, std=0.5)
        .add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS))
        .add_egress(KafkaEgress(
            telemetry_topic="sim-telemetry",
            event_topic="sim-events",
            bootstrap_servers=BOOTSTRAP_SERVERS,
        ))
    )

    # 4. Define Simulation Logic using Decorators
    @app.arrival_loop("standard")
    def arrival_process(context: SimulationContext):
        task_id = 0
        while True:
            yield context.wait_for_arrival("standard")
            context.spawn(work_task(task_id))
            task_id += 1

    @app.task(service_id="milling", resource_id="lathe")
    def work_task(task_id: int):
        # We can return a Pydantic model directly!
        return TaskEvent(path_id="Line_A.service.milling", status="finished")

    @app.telemetry_loop(interval=2.0)
    def telemetry_monitor(context: SimulationContext):
        res = context.get_resource("lathe")
        context.env.publish_telemetry("Line_A.lathe.capacity", res.capacity)
        context.env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))

    # 5. Run the Simulation
    print("Simulation started. Listening to Kafka...")
    try:
        app.run()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    run()
```
