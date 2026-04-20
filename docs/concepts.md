# Core Concepts

**Dynamic DES** was built to solve a fundamental problem with traditional Discrete-Event Simulation (DES): once a simulation starts, its parameters are usually locked in memory. If you want to connect a simulation to a live data feed (like a Kafka stream), you run the risk of blocking the simulation clock while waiting for network I/O.

To solve this, Dynamic DES uses a highly decoupled, asynchronous architecture.

## Switchboard Pattern

Instead of simulation resources polling Kafka directly, the architecture is split into three isolated layers:

1. **Connectors (Ingress/Egress)**: Background `asyncio` threads handle heavy network I/O (Kafka, Redis) independently of the simulation clock.
2. **Registry (Switchboard)**: A centralized state manager that flattens nested configurations into dot-notation paths (e.g., `Line_A.lathe.capacity`). It acts as the single source of truth.
3. **Resources (SimPy Objects)**: Passive observers. They do not poll for data. Instead, they "wake up" only when the Registry signals a specific path has changed.

This pattern ensures that network latency or dropped connections never freeze the internal `SimPy` clock.

## Event-Driven Capacity

Standard SimPy resources have static capacities. If you create a `simpy.Resource(capacity=5)`, it is forever 5.

Dynamic DES introduces `DynamicResource`, `DynamicContainer`, and `DynamicStore`. These wrap native SimPy objects and listen directly to the Registry.

When the control plane dictates a capacity change:

- **Growing is easy:** If capacity increases from 5 to 10, 5 new tokens are immediately added to the pool, instantly waking up any pending requests.
- **Shrinking is complex:** If capacity shrinks from 5 to 2, but 4 tasks are currently processing, what happens? **Dynamic DES never destroys Work-In-Progress (WIP).** The resource enters a temporary "over-capacity" state. It will wait for tasks to naturally finish and release their tokens, intercepting those tokens until the physical pool matches the new, smaller capacity limit.

## High-Throughput Egress & Batching

Sending an individual HTTP/Kafka request for every single simulation event (e.g., a part moving one millimeter on a conveyor) would instantly crash the network.

To handle high throughput, the `EgressMixIn` buffers events in a thread-safe queue.

- Events are pushed to the background network thread in **batches** (e.g., 500 at a time).
- The queue flushes automatically if the `flush_interval` (e.g., 1 second) is reached to ensure real-time UI dashboards don't feel "laggy".

## Duck-Typing and Pluggable Serialization

Dynamic DES enforces a strict separation of concerns between your **simulation logic** and your **data infrastructure**.

Your simulation code shouldn't care if a data science team wants Avro messages or if a frontend team wants JSON. Using **Duck-Typing**, you can yield native `Pydantic` models directly to the environment:

```python
# In your simulation logic:
env.publish_event("task-1", TaskEvent(status="finished"))
```

The Egress Connectors handle the rest using a **Strategy Pattern**. The connector detects the Python object, automatically extracts the dictionary via `.model_dump()`, routes it to the correct topic, and applies the designated serializer (JSON by default, or Avro if configured via Confluent/AWS Glue Schema Registries).
