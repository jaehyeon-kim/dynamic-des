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

## Project Layout

- [Getting Started](getting-started.md): A walkthrough of the local example.
- [Core Concepts](concepts.md): Learn about the Switchboard Pattern.
- [API Reference](api.md): Detailed documentation of classes and methods.
