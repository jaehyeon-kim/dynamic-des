# Core Concepts

**Dynamic DES** is built on the **Switchboard Pattern**, decoupling data sourcing from simulation logic.

## Switchboard Pattern

Instead of resources polling Kafka directly, the architecture is split into three layers:

1.  **Connectors (Ingress/Egress)**: Background threads handle heavy I/O (Kafka, Redis).
2.  **Registry (Switchboard)**: A centralized state manager that flattens data into dot-notation paths.
3.  **Resources (SimPy Objects)**: Passive observers that "wake up" only when the Registry signals a change.

## Event-Driven Capacity

Standard SimPy resources have static capacities. `DynamicResource` wraps a `Container` and a `PriorityStore`. When the Registry updates:

- **Growing**: Extra tokens are added to the pool immediately.
- **Shrinking**: The resource requests tokens back. If they are busy, it waits until they are released, ensuring no work-in-progress is lost.

## High-Throughput Events

To handle high throughput, the `EgressMixIn` uses:

- **Batching**: Pushing lists of events to the I/O thread to reduce lock contention.
- **orjson**: Rust-powered serialization for maximum speed.
