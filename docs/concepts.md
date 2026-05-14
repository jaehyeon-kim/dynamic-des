# Core Concepts

**Dynamic DES** was built to solve a fundamental problem with traditional Discrete-Event Simulation (DES): once a simulation starts, its parameters are usually locked in memory. If you want to connect a simulation to a live data feed (like a Kafka stream), you run the risk of blocking the simulation clock while waiting for network I/O.

To solve this, Dynamic DES uses a highly decoupled, asynchronous architecture.

## Switchboard Pattern

Instead of simulation resources polling Kafka directly, the architecture is split into three isolated layers:

1. **Connectors (Ingress/Egress)**: Background `asyncio` threads handle heavy network I/O (Kafka, Redis) or File I/O (S3, local disk) independently of the simulation clock.
2. **Registry (Switchboard)**: A centralized state manager that flattens nested configurations into dot-notation paths (e.g., `Line_A.lathe.capacity`). It acts as the single source of truth.
3. **Resources (SimPy Objects)**: Passive observers. They do not poll for data. Instead, they "wake up" only when the Registry signals a specific path has changed.

This pattern ensures that network latency or dropped connections never freeze the internal `SimPy` clock.

## Synchronized Forecasting & Temporal Decoupling

While real-time synchronization (`factor=1.0`) is perfect for live digital twins, Dynamic DES can also detach its logical simulation clock from the real-world wall clock by setting `factor=0.0`. 

This transforms your model into a **Synchronized Forecasting Engine**. 

By querying your live production database for the exact current state of your factory (e.g., current capacities, queue lengths, active tasks) and injecting them into the `SimParameter` registry, you can fast-forward the simulation to instantly predict what the system will look like 8 hours into the future. Because the architecture remains identical, the exact same simulation logic scales seamlessly from real-time monitoring to high-speed batch forecasting.

## Event-Driven Capacity

Standard SimPy resources have static capacities. If you create a `simpy.Resource(capacity=5)`, it is forever 5.

Dynamic DES introduces `DynamicResource`, `DynamicContainer`, and `DynamicStore`. These wrap native SimPy objects and listen directly to the Registry.

When the control plane dictates a capacity change:

- **Growing is easy:** If capacity increases from 5 to 10, 5 new tokens are immediately added to the pool, instantly waking up any pending requests.
- **Shrinking is complex:** If capacity shrinks from 5 to 2, but 4 tasks are currently processing, what happens? **Dynamic DES never destroys Work-In-Progress (WIP).** The resource enters a temporary "over-capacity" state. It will wait for tasks to naturally finish and release their tokens, intercepting those tokens until the physical pool matches the new, smaller capacity limit.

## High-Throughput Egress & Batching

Sending an individual HTTP/Kafka request (or performing a disk write) for every single simulation event would instantly crash the network or I/O limits.

To handle high throughput, the `EgressMixIn` buffers events in a thread-safe queue.

- Events are pushed to the background network/file thread in **batches** (e.g., 5000 at a time).
- In real-time mode, the queue flushes automatically if the `flush_interval` (e.g., 1 second) is reached to ensure UI dashboards don't feel "laggy".
- In fast-forward forecasting mode, these large batches are crucial for generating highly compressed Data Lake files.

## Data Lake Ingestion & Schema Stability

When running forecasting engines, you often generate millions of records that need to be stored in Data Lakes (AWS S3, Google Cloud Storage, Databricks). Dynamic DES utilizes PyArrow VFS to natively support object storage. 

To support massive parallel processing downstream, Dynamic DES enforces two key storage concepts:
1. **UUID Chunking**: Every time a batch flushes, it is written to a uniquely named chunk (e.g., `events_a1b2c3d4.parquet`). This prevents file locking and overwrite collisions.
2. **Schema Drift Prevention**: When writing Parquet datasets, the framework infers the schema from the very first batch. If a subsequent batch introduces schema drift (e.g., a float unexpectedly arrives as an integer), the egress connector intercepts it and automatically casts it to match the canonical schema, ensuring your analytical pipelines never break.

## Duck-Typing and Pluggable Serialization

Dynamic DES enforces a strict separation of concerns between your **simulation logic** and your **data infrastructure**.

Your simulation code shouldn't care if a data science team wants Avro messages in Kafka, or if a data engineering team wants Parquet files in S3. Using **Duck-Typing**, you can yield native `Pydantic` models directly to the environment:

```python
# In your simulation logic:
env.publish_event("task-1", TaskEvent(status="finished"))
```

The Egress Connectors handle the rest using a **Strategy Pattern**. The connector detects the Python object, automatically extracts the dictionary via `.model_dump()`, routes it to the correct topic or file path, and applies the designated serialization format (JSON, Avro, or Parquet).
