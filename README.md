# Dynamic DES

[![CI Pipeline](https://github.com/jaehyeon-kim/dynamic-des/actions/workflows/pipeline.yml/badge.svg)](https://github.com/jaehyeon-kim/dynamic-des/actions/workflows/pipeline.yml)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://jaehyeon-kim.github.io/dynamic-des/)
[![PyPI version](https://badge.fury.io/py/dynamic-des.svg)](https://badge.fury.io/py/dynamic-des)
[![Python Versions](https://img.shields.io/pypi/pyversions/dynamic-des.svg)](https://pypi.org/project/dynamic-des/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Real-time SimPy control plane for event-driven digital twins.**

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/docs/assets/dashboard-preview.gif" alt="Dashboard Screenshot" width="800" />
</div>

Dynamic DES bridges the gap between static discrete-event simulations and the live world. It allows you to update simulation parameters (arrivals, service times, capacities) and stream telemetry via **Kafka**, **Redis**, or **PostgreSQL** without stopping the simulation.

## Key Features

- **⚡ Real-Time Control**: Synchronize SimPy with the system clock using `DynamicRealtimeEnvironment`.
- **🔗 Dynamic Registry**: Dynamic, path-based updates (e.g., `Line_A.arrival.rate`) that trigger instant logic changes.
- **🚀 High Throughput**: Optimized to handle high throughput using `orjson` and local batching.
- **🔋 Flexible Resources**: `DynamicResource` provides prioritized queuing with graceful capacity shrinking.
- **🔌 Modular Connectors**: Plugin-based architecture for Kafka, Redis, Postgres and Local testing.
- **📊 System Observability**: Built-in lag monitoring to track simulation drift from real-world time, exposed via the telemetry stream.

## Installation

Install the core library:

```bash
pip install dynamic-des
```

To include specific backends:

```bash
# For Kafka support
pip install "dynamic-des[kafka]"

# For Kafka and Dashboard support
pip install "dynamic-des[kafka,dashboard]"

# For all backends (Kafka, Redis, Postgres, Dashboard)
pip install "dynamic-des[all]"
```

## Quick Start: Zero-Setup Demos

Dynamic DES comes with built-in examples and infrastructure orchestration so you can see it in action immediately.

**Run the local, dependency-free simulation:**

```bash
ddes-local-example
```

**Run the full Real-Time Digital Twin stack with Kafka and a live UI:**

```bash
# Start the background Kafka cluster (requires Docker)
ddes-kafka-infra-up

# Open a new terminal and run the simulation
# Ctrl + C to stop
ddes-kafka-example

# Open a new terminal and start the control dashboard (opens in browser)
# Visit http://localhost:8080
# Ctrl + C to stop
ddes-kafka-dashboard

# Clean up the infrastructure when finished
ddes-kafka-infra-down
```

## Building Your Own Simulation (Local Example)

The following snippet demonstrates a simple example. It initializes a production line, schedules an external capacity update, and streams telemetry to the console.

```python
import logging
import numpy as np
from dynamic_des import (
    CapacityConfig, ConsoleEgress, DistributionConfig,
    DynamicRealtimeEnvironment, DynamicResource, LocalIngress, SimParameter
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
logger = logging.getLogger("local_example")

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
        env.publish_telemetry("Line_A.resources.lathe.capacity", res.capacity)
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

### Data Egress JSON Schemas

To ensure strict data contracts with external consumers (like Kafka, Redis, or PostgreSQL), `dynamic-des` uses Pydantic to validate all outbound payloads. Users can expect two distinct JSON structures depending on the stream type:

#### Telemetry Stream

Used for scalar metrics like resource utilization, queue lengths, or simulation lag.

```json
{
  "stream_type": "telemetry",
  "path_id": "Line_A.resources.lathe.utilization",
  "value": 85.5,
  "sim_ts": 120.5,
  "timestamp": "2023-10-25T14:30:00.000Z"
}
```

#### Event Stream

Used for discrete task lifecycle events (e.g., a part arriving, entering a queue, or finishing processing).

```json
{
  "stream_type": "event",
  "key": "task-001",
  "value": {
    "status": "finished",
    "duration": 45.2,
    "path_id": "Line_A.service.lathe"
  },
  "sim_ts": 125.0,
  "timestamp": "2023-10-25T14:30:04.500Z"
}
```

### More Examples

For more examples, including implementations using **Kafka** providers, please explore the [examples](./src/dynamic_des/examples/) folder.

## Core Concepts

**Dynamic DES** is built on the **Switchboard Pattern**, decoupling data sourcing from simulation logic.

### Switchboard Pattern

Instead of resources polling Kafka directly, the architecture is split into three layers:

1.  **Connectors (Ingress/Egress)**: Background threads handle heavy I/O (Kafka, Redis).
2.  **Registry (Switchboard)**: A centralized state manager that flattens data into dot-notation paths.
3.  **Resources (SimPy Objects)**: Passive observers that "wake up" only when the Registry signals a change.

### Event-Driven Capacity

Standard SimPy resources have static capacities. `DynamicResource` wraps a `Container` and a `PriorityStore`. When the Registry updates:

- **Growing**: Extra tokens are added to the pool immediately.
- **Shrinking**: The resource requests tokens back. If they are busy, it waits until they are released, ensuring no work-in-progress is lost.

### High-Throughput Events

To handle high throughput, the `EgressMixIn` uses:

- **Batching**: Pushing lists of events to the I/O thread to reduce lock contention.
- **orjson**: Rust-powered serialization for maximum speed.

## Documentation

For full documentation, architecture details, and API reference, visit:
[https://jaehyeon.me/dynamic-des/](https://jaehyeon.me/dynamic-des/).

## License

MIT
