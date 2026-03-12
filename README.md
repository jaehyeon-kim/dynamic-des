# Dynamic DES

**Real-time SimPy control plane for event-driven digital twins.**

Dynamic DES bridges the gap between static discrete-event simulations and the live world. It allows you to update simulation parameters (arrivals, service times, capacities) and stream telemetry via **Kafka**, **Redis**, or **PostgreSQL** without stopping the simulation.

---

## Key Features

- **⚡ Real-Time Control**: Synchronize SimPy with the system clock using `DynamicRealtimeEnvironment`.
- **🔗 Dynamic Registry**: Dynamic, path-based updates (e.g., `Line_A.arrival.rate`) that trigger instant logic changes.
- **🚀 High Throughput**: Optimized to handle high throughput using `orjson` and local batching.
- **🔋 Flexible Resources**: `DynamicResource` provides prioritized queuing with graceful capacity shrinking.
- **🔌 Modular Connectors**: Plugin-based architecture for Kafka, Redis, Postgres and Local testing.

---

## Installation

Install the core library:

```bash
pip install dynamic-des
```

To include specific backends:

```bash
# For Kafka support
pip install "dynamic-des[kafka]"

# For all backends (Kafka, Redis, Postgres)
pip install "dynamic-des[all]"
```

---

## Quick Start

The following snippet demonstrates a simple example. It initializes a production line, schedules an external capacity update, and streams telemetry to the console.

```python
import numpy as np
from dynamic_des import (
    CapacityConfig, ConsoleEgress, DistributionConfig,
    DynamicRealtimeEnvironment, DynamicResource, LocalIngress, SimParameter
)

# 1. Define initial system state
params = SimParameter(
    sim_id="Line_A",
    arrival={"standard": DistributionConfig(dist="exponential", rate=1)},
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},
)

# 2. Setup Environment with Local Connectors
# Schedule capacity to jump from 1 to 3 at t=5s
ingress = LocalIngress([(5.0, "Line_A.resources.lathe.current_cap", 3)])
egress = ConsoleEgress()

env = DynamicRealtimeEnvironment(factor=1.0)
env.registry.register_sim_parameter(params)
env.setup_ingress([ingress])
env.setup_egress([egress])

# 3. Create Resource
res = DynamicResource(env, "Line_A", "lathe")

def telemetry_monitor(env: DynamicRealtimeEnvironment, res: DynamicResource):
    """Streams system health metrics every 2 seconds."""
    while True:
        env.publish_telemetry("Line_A.resources.lathe.capacity", res._capacity)
        yield env.timeout(2.0)


env.process(telemetry_monitor(env, res))

# 4. Run
print("Simulation started. Watch capacity change at t=5s...")
try:
    env.run(until=10.1)
finally:
    env.teardown()
```

### What this does

1.  **Registry Initialization**: The `SimParameter` defines the initial state. The Registry flattens this into addressable paths (e.g., `Line_A.resources.lathe.current_cap`).
2.  **Live Ingress**: The `LocalIngress` simulates an external event (like a Kafka message) arriving 5 seconds into the run.
3.  **Zero-Polling Update**: The `DynamicResource` listens to the Registry. The moment the ingress updates the value, the resource automatically expands its internal token pool without any manual checking.
4.  **Telemetry Egress**: The `ConsoleEgress` prints system vitals to your terminal, mimicking a live dashboard feed.

### More Examples

For more examples, including implementations using **Kafka** providers, please explore the [examples](./src/dynamic_des/examples/) folder.

---

## Core Concepts

### Switchboard Pattern

Instead of resources polling Kafka directly, Dynamic DES uses a centralized **Registry**.

1. **Connectors** push data from Kafka/Redis/Postgres into the Registry.
2. **Registry** flattens data into addressable paths and triggers SimPy events.
3. **Resources** are passive observers that wake up only when their specific path changes.

### Capacity Management

`DynamicResource` ensures physical integrity. When capacity is reduced via an external update, the resource waits for busy tasks to finish before removing tokens from the pool, preventing loss of work-in-progress.

---

## Documentation

For full documentation, architecture details, and API reference, visit:
[https://jaehyeon.me/dynamic-des/](https://jaehyeon.me/dynamic-des/).

## License

MIT
