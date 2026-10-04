# Dynamic DES

**Dynamic DES** is a high-performance, real-time control plane for [SimPy](https://simpy.readthedocs.io/).

It bridges the gap between static discrete-event simulations and the live world by allowing you to update simulation parameters (arrivals, service times, capacities) and stream telemetry and events via **Kafka**, **Redis**, or **PostgreSQL** without stopping the simulation. It also transforms your models into **synchronized forecasting engines** by fast-forwarding through simulation time to predict future states or backfill Data Lakes with schema-enforced **Parquet** or **JSONL** files directly to local storage or **S3-compatible storage such as AWS S3 or SeaweedFS** using PyArrow VFS, or commit the same run into an **Apache Iceberg** table through a REST catalog.

<div align="center">
  <img src="assets/architecture.png" alt="Dynamic DES architecture" width="900" />
</div>

---

## Key Features

- **⚡ Real-Time Control**: Synchronize SimPy with the system clock using `DynamicRealtimeEnvironment`.
- **🔗 Builder Pattern**: Construct digital twins declaratively with `SimulationContext` and decorators like `@app.task`.
- **🧾 YAML Blueprints**: Declare parameters, connectors and timed experiments in a YAML file and run it with `ddes run`, with logic kept in Python and referenced through `!python`.
- **🔗 Dynamic Registry**: Dynamic, path-based updates (e.g., `Line_A.arrival.standard.rate`) that trigger instant logic changes.
- **🛡️ Enterprise Ready**: Native `**kwargs` passthrough for SASL, mTLS, OAuth, and AWS IAM Kafka clusters.
- **📦 Pluggable Serialization**: Stream lightweight JSON by default, or map specific ML topics to lazy-loaded **Avro/Schema Registry** serializers.
- **🗄️ Data Lake Ready**: Write chunked Parquet and JSONL datasets directly to object storage via PyArrow VFS, with built-in schema inference and drift prevention.
- **🧊 Lakehouse Ready**: Append straight into an Apache Iceberg table through an Iceberg REST catalog, with one commit per flush so the snapshot count stays under your control.
- **🦆 Pydantic Duck-Typing**: Seamlessly publish strictly-typed Pydantic V2 models straight from your simulation logic.
- **📊 System Observability**: Built-in lag monitoring to track simulation drift from real-world time.

---

## Documentation Layout

* **[Getting Started](getting-started.md)**: Install, download the examples, and start the containers they need.
* **Tutorials**, one factory written three ways:
    * **[Part 1: Low-level API](tutorials/low-level.md)**: Build the factory on `DynamicRealtimeEnvironment`, with SimPy processes started by `env.process`.
    * **Part 2: Declarative API**:
        * **[1. Your First Factory (Local)](tutorials/01-first-factory.md)**: Define a local factory lifecycle.
        * **[2. Adding Randomness and Rules](tutorials/02-distributions-resources.md)**: Add stochastic distributions and ingress scheduled capacity updates.
        * **[3. Going Distributed (Kafka)](tutorials/03-connecting-kafka.md)**: Connect standard simulation logic to live Kafka streams.
    * **[Part 3: YAML](tutorials/yaml.md)**: Write the same factory as a blueprint and run it with `ddes run`.
* **Core Architecture**:
    * **[Overview](architecture/overview.md)**: The three ways to write a simulation, and the parameters and environment they share.
    * **Writing a simulation**: [Low-level API](architecture/low-level.md), [Declarative API](architecture/context.md), [YAML Blueprints](architecture/yaml.md).
    * **Runtime**: [Realtime Environment](architecture/environment.md), [Registry and Live Parameters](architecture/registry.md), [Time](architecture/time.md), [Resources and Containers](architecture/resources.md), [Connectors](architecture/connectors.md), [Records and Telemetry](architecture/records.md), [Batching and Delivery](architecture/batching.md).
* **Guides**:
    * **Connectors**: [Complex Routing (Kafka)](guides/complex-routing.md), [Advanced Serialization (Avro and Pydantic)](guides/avro-and-pydantic.md), [Connecting to Secure Kafka](guides/kafka-security.md).
    * **Features**: [Backfill Then Go Live in One Run](guides/backfill-then-live.md), [Change Parameters While a Simulation Runs](guides/live-parameters.md).
    * **YAML**: [YAML Blueprints, from First File to Connectors](guides/yaml-blueprints.md).
* **Advanced**: [Advanced YAML: Custom Logic with `!python`](guides/yaml-advanced.md), [Multi-Resource Handoffs](guides/multi-resource-handoffs.md), [Preemptive Machine Breakdowns](guides/preemptive-breakdowns.md), [Absolute Edge Cases (Dynamic Topology)](guides/dynamic-topology.md).
* **Examples**, every script in the `examples/` folder, one page each with a tab for the declarative API, the low-level API and the YAML blueprint: [Local](examples/local.md), [Kafka](examples/kafka.md), [Parquet](examples/parquet.md), [Iceberg](examples/iceberg.md), [Postgres](examples/postgres.md), [Redis](examples/redis.md), [Backfill Then Go Live](examples/backfill-live.md), [Orders with Line Items (Advanced YAML)](examples/advanced-postgres-orders.md).
* **[API Reference](api.md)**: Technical reference for all public classes.
