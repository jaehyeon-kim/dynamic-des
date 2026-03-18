# Dynamic DES

**Dynamic DES** is a high-performance, real-time control plane for [SimPy](https://simpy.readthedocs.io/).

It bridges the gap between static discrete-event simulations and the live world by allowing you to update simulation parameters (arrivals, service times, capacities) and stream telemetry and events via **Kafka**, **Redis**, or **PostgreSQL** without stopping the simulation.

## Key Features

- **⚡ Real-Time Control**: Synchronize SimPy with the system clock using `DynamicRealtimeEnvironment`.
- **🔗 Dynamic Registry**: Dynamic, path-based updates (e.g., `Line_A.arrival.rate`) that trigger instant logic changes.
- **🚀 High Throughput**: Optimized to handle high throughput using `orjson` and local batching.
- **🔋 Flexible Resources**: `DynamicResource` provides prioritized queuing with graceful capacity shrinking.
- **🔌 Modular Connectors**: Plugin-based architecture for Kafka, Redis, Postgres and Local testing.
- **📊 System Observability**: Built-in lag monitoring to track simulation drift from real-world time, exposed via the telemetry stream.

## Installation

Install the core library using `pip` or `uv`:

```bash
pip install dynamic-des
```

To include specific backends:

```bash
# For Kafka support
pip install "dynamic-des[kafka]"

# For all backends
pip install "dynamic-des[all]"
```

## Quick Start

```python
from dynamic_des import DynamicRealtimeEnvironment, DynamicResource

# Initialize the real-time environment
env = DynamicRealtimeEnvironment(factor=1.0)

# The resource will automatically listen to its registry path
resource = DynamicResource(env, sim_id="Line_A", resource_id="lathe")

# Start your simulation
env.run(until=100)
```

## Project Layout

- [Getting Started](getting-started.md): A walkthrough of the local example.
- [Core Concepts](concepts.md): Learn about the Switchboard Pattern.
- [API Reference](api.md): Detailed documentation of classes and methods.
