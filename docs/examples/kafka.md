# Kafka Digital Twin

This example demonstrates how to integrate `dynamic-des` into a full event-driven architecture.

By replacing the Local connectors with `KafkaIngress` and `KafkaEgress`, the simulation becomes a fully detached microservice. It listens for external JSON commands to mutate its state, and streams telemetry and strictly-typed Pydantic events to outbound topics.

## Execution

To run this example, you need the Kafka and Dashboard extras installed. You will also need **Docker** running to host the local Kafka infrastructure.

**1. Install Dependencies**

```bash
pip install "dynamic-des[kafka,dashboard]"
```

**2. Start Infrastructure**

```bash
# Spin up Dockerized Kafka and Zookeeper
ddes-kafka-infra-up
```

**3. Run the Simulation**

```bash
# In a new terminal, start the simulation engine
ddes-kafka-example
```

**4. Launch the Dashboard**

```bash
# In another terminal, start the UI (opens at http://localhost:8080)
ddes-kafka-dashboard
```

**5. Cleanup**

```bash
# When finished, shut down the containers
ddes-kafka-infra-down
```

## Code

This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

```python
import logging
import time
import numpy as np
from pydantic import BaseModel

from dynamic_des import (
    CapacityConfig, DistributionConfig, DynamicRealtimeEnvironment,
    DynamicResource, KafkaAdminConnector, KafkaEgress, KafkaIngress,
    Sampler, SimParameter,
)

logging.basicConfig(level=logging.INFO)

# 1. Define Strongly-Typed Event Payloads
class TaskEvent(BaseModel):
    """
    Thanks to duck-typing, we can pass this Pydantic model directly into
    env.publish_event(). The KafkaEgress layer handles the extraction!
    """
    path_id: str
    status: str

def run():
    BOOTSTRAP_SERVERS = "localhost:9092"

    # 2. Bootstrap Kafka Topics
    admin_connector = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS)
    admin_connector.create_topics([
        {"name": "sim-config"}, {"name": "sim-telemetry"}, {"name": "sim-events"}
    ])
    time.sleep(2)

    # 3. Define initial system state
    line_a_params = SimParameter(
        sim_id="Line_A",
        arrival={"standard": DistributionConfig(dist="exponential", rate=1.0)},
        service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
        resources={"lathe": CapacityConfig(current_cap=1, max_cap=10)},
    )

    # 4. Setup Environment with Kafka Connectors
    ingress = KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS)
    egress = KafkaEgress(
        telemetry_topic="sim-telemetry",
        event_topic="sim-events",
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(line_a_params)
    env.setup_ingress([ingress])
    env.setup_egress([egress])

    res = DynamicResource(env, "Line_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(42))

    # 5. Define Simulation Logic
    def arrival_process(env, res):
        arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(arrival_cfg))
            env.process(work_task(env, task_id, res, "Line_A.service.milling"))
            task_id += 1

    def work_task(env, task_id, res, path_id):
        task_key = f"task-{task_id}"

        # Publish Pydantic model instead of raw dictionary
        env.publish_event(task_key, TaskEvent(path_id=path_id, status="queued"))

        with res.request() as req:
            yield req
            env.publish_event(task_key, TaskEvent(path_id=path_id, status="started"))

            service_cfg = env.registry.get_config(path_id)
            yield env.timeout(sampler.sample(service_cfg))

            env.publish_event(task_key, TaskEvent(path_id=path_id, status="finished"))

    def telemetry_monitor(env, res):
        while True:
            env.publish_telemetry("Line_A.lathe.capacity", res.capacity)
            env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))
            yield env.timeout(2.0)

    # 6. Run
    env.process(arrival_process(env, res))
    env.process(telemetry_monitor(env, res))

    print("Simulation started. Listening to Kafka...")
    try:
        env.run()
    except KeyboardInterrupt:
        pass
    finally:
        env.teardown()

if __name__ == "__main__":
    run()
```
