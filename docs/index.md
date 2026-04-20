# Dynamic DES

**Dynamic DES** is a high-performance, real-time control plane for [SimPy](https://simpy.readthedocs.io/).

It bridges the gap between static discrete-event simulations and the live world by allowing you to update simulation parameters (arrivals, service times, capacities) and stream telemetry and events via **Kafka**, **Redis**, or **PostgreSQL** without stopping the simulation.

## Key Features

- **⚡ Real-Time Control**: Synchronize SimPy with the system clock using `DynamicRealtimeEnvironment`.
- **🔗 Dynamic Registry**: Dynamic, path-based updates (e.g., `Line_A.arrival.rate`) that trigger instant logic changes.
- **🛡️ Enterprise Ready**: Native `**kwargs` passthrough for SASL, mTLS, OAuth, and AWS IAM Kafka clusters.
- **📦 Pluggable Serialization**: Stream lightweight JSON by default, or map specific ML topics to lazy-loaded **Avro/Schema Registry** serializers.
- **🦆 Pydantic Duck-Typing**: Seamlessly publish strictly-typed Pydantic V2 models straight from your simulation logic.
- **🔋 Flexible Resources**: `DynamicResource` provides prioritized queuing with graceful capacity shrinking.
- **📊 System Observability**: Built-in lag monitoring to track simulation drift from real-world time.

## Documentation Layout

- [Getting Started](getting-started.md): Quick installation and zero-setup demo commands.
- [Core Concepts](concepts.md): Deep dive into the architecture, the Switchboard Pattern, and the shrinking paradox.
- **Examples**
    - [Local Simulation](examples/local.md): A dependency-free approach to testing and deterministic scheduling.
    - [Kafka Digital Twin](examples/kafka.md): A full event-driven architecture showcasing dynamic state updates and Pydantic event routing.
- **Advanced Guides**
    - [Cluster Authentication](guides/kafka-security.md): Connect to enterprise environments natively (SASL, mTLS, OAuth, and AWS IAM).
    - [Avro Schemas & Pydantic](guides/avro-and-pydantic.md): Leverage pluggable serialization, duck-typing, and schema registries (Confluent & AWS Glue).
- [API Reference](api.md): Detailed documentation of classes and methods.
