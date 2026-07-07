# Tutorial 3: Going Distributed (Kafka)

In this final basics tutorial, you will transition your simulation into a distributed microservice. You will swap local console connectors for Kafka connectors, allowing you to stream telemetry and receive live parameter updates over the network.

---

## 1. Prerequisites and Installation

First, make sure you install the `kafka` optional dependencies:
```bash
pip install "dynamic-des[kafka]"
```

Bring up a local Kafka cluster (or use the built-in orchestration helper):
```bash
ddes-kafka-infra-up
```

---

## 2. Transitioning to Kafka Connectors

Swapping from a local script to a distributed twin is simple: we replace `ConsoleEgress` and `LocalIngress` with `KafkaEgress` and `KafkaIngress` respectively.

Update your configuration code:

```python
import logging
from dynamic_des import SimulationContext, KafkaEgress, KafkaIngress

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s")

# Connect to the local Kafka broker
broker = "localhost:9092"
sim_id = "Factory_A"

app = (
    SimulationContext(sim_id=sim_id, factor=1.0, random_seed=42)
    .add_resource("lathe", current_cap=1, max_cap=5)
    .add_arrival("standard", dist="exponential", rate=0.5)
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    # 1. Add Kafka Ingress to listen to dynamic capacity updates
    .add_ingress(KafkaIngress(
        bootstrap_servers=broker,
        topic=f"{sim_id}-control"
    ))
    # 2. Add Kafka Egress to publish simulation events
    .add_egress(KafkaEgress(
        bootstrap_servers=broker,
        event_topic=f"{sim_id}-events",
        telemetry_topic=f"{sim_id}-telemetry"
    ))
)
```

---

## 3. Keep the Core Logic Identical

Because the standard API decouples infrastructure from business logic, **you do not need to modify any of the simulation generators or task loops** from Tutorial 2:

```python
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
```

---

## 4. Testing Live Modifications

Start the simulation in your terminal. It will block and run in real time matching the system clock:
```python
if __name__ == "__main__":
    print("Distributed twin running. Press Ctrl+C to stop.")
    app.run()
```

While it is running, you can publish a message to the `Factory_A-control` Kafka topic to dynamically scale the lathe capacity:

```json
{
  "path_id": "Factory_A.resources.lathe.current_cap",
  "value": 3
}
```

The background `KafkaIngress` thread will instantly consume this message, apply the update to the switchboard registry, and the active `DynamicResource` lathe will expand its capacity without stopping the clock or resetting any states.
