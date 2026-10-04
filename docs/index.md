# Dynamic DES

Dynamic DES runs [SimPy](https://simpy.readthedocs.io/) discrete-event simulations in step with the system clock, or as fast as the machine allows. A running simulation takes parameter changes (arrival rates, service times, capacities) from **Kafka**, **Redis**, **PostgreSQL** or a timed scenario, without stopping. Its task events and telemetry go to the sinks you attach: **Kafka**, **Redis**, **PostgreSQL**, **Parquet** or **JSONL** files on local disk or **S3-compatible storage** such as AWS S3 or SeaweedFS, or an **Apache Iceberg** table through a REST catalog.

A simulation can be written three ways: with the low-level `DynamicRealtimeEnvironment`, with the declarative `SimulationContext` builder, or as a plain **YAML blueprint** run with the `ddes` command. One run can generate backdated history at full speed and then continue in real time, so the same model can fill a data lake and then feed a live system.

<div align="center">
  <img src="assets/architecture.png" alt="Dynamic DES architecture" width="900" />
</div>

---

## Key Features

- **⚡ Real-Time Control**: Synchronize SimPy with the system clock using `DynamicRealtimeEnvironment`.
- **🧭 Three Ways to Write a Simulation**: The low-level `DynamicRealtimeEnvironment`, the declarative `SimulationContext` builder, or a YAML blueprint. All three build the same parameters and run on the same environment.
- **🧾 YAML Blueprints**: Declare parameters, connectors, tasks, telemetry and timed experiments in a plain YAML file and run it with `ddes run`. Logic that YAML cannot express stays in Python and is referenced through `!python`.
- **⏩ Backfill Then Go Live**: One run generates backdated history unpaced, then switches to real time at `go_live_at`, with one seed and one seam.
- **🔀 Several Sinks per Run**: Attach a stream sink and a lake sink to one run, each with its own `when` predicate, `batch_size` and `flush_interval` on `add_egress`.
- **🔗 Dynamic Registry**: Dynamic, path-based updates (e.g., `Line_A.arrival.standard.rate`) that trigger instant logic changes.
- **🚀 High Throughput**: Optimized to handle high throughput using `orjson` and local batching.
- **🛡️ Enterprise Ready**: Native `**kwargs` passthrough for SASL, mTLS, OAuth, and AWS IAM Kafka clusters.
- **📦 Pluggable Serialization**: Stream lightweight JSON by default, or map specific ML topics to lazy-loaded **Avro/Schema Registry** serializers (Confluent & AWS Glue).
- **🗄️ Data Lake Ingestion**: Native PyArrow VFS integration for fast chunked writing (Parquet/JSONL) directly to object storage, with built-in schema inference and drift enforcement.
- **🧊 Lakehouse Ingestion**: Append straight into an Apache Iceberg table through an Iceberg REST catalog, with one commit per flush so the snapshot count stays under your control.
- **🦆 Pydantic Duck-Typing**: Seamlessly publish strictly-typed Pydantic V2 models straight from your simulation logic.
- **📊 System Observability**: Built-in lag monitoring to track simulation drift from real-world time.
- **🌍 Domain Agnostic**: Perfect for factory floors, crypto trading bots, or RPG game state management.

---

## Documentation Layout

* **[Getting Started](getting-started.md)**: Install, download the examples, and start the containers they need.
* **Tutorials**, one factory written three ways:
    * **[Part 1: Low-level API](tutorials/low-level.md)**: Build the factory on `DynamicRealtimeEnvironment`, with SimPy processes started by `env.process`.
    * **[Part 2: Declarative API](tutorials/declarative.md)**: Build the factory with `SimulationContext`, add randomness and scheduled capacity changes, then connect it to Kafka.
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
* **Examples**, one page per example, with a tab for each way it is written. Backfill Then Go Live has declarative and YAML tabs, and the advanced orders example is YAML only: [Local](examples/local.md), [Kafka](examples/kafka.md), [Parquet](examples/parquet.md), [Iceberg](examples/iceberg.md), [Postgres](examples/postgres.md), [Redis](examples/redis.md), [Backfill Then Go Live](examples/backfill-live.md), [Orders with Line Items (Advanced YAML)](examples/advanced-postgres-orders.md).
* **[API Reference](api.md)**: Technical reference for all public classes.
