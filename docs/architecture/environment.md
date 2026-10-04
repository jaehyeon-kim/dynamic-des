# Realtime Environment

At the heart of every simulation run is the `DynamicRealtimeEnvironment`, which extends SimPy's core environment. It is responsible for controlling the logical clock, managing simulated process tasks, and coordinating temporal synchronization. It holds the [registry](registry.md) at `env.registry`, and runs the ingress and egress connectors on background threads. [Time](time.md) describes the clock and its pacing.

---

## Threading and Asynchronous I/O

Standard SimPy environments are single-threaded and synchronous. Performing heavy network operations (like writing to a Kafka broker or reading from S3) directly inside a SimPy process would block the entire simulation loop.

To resolve this, the `DynamicRealtimeEnvironment` coordinates background operations:

```text
┌─────────────────────────────────────────────────────────┐
│              SimPy Environment Thread                   │
│  - Runs discrete event loop                             │
│  - Increments simulation clock                          │
│  - Buffers records, hands full batches to egress queues │
└──────────────────────────┬──────────────────────────────┘
                           │ Thread-Safe Queue
                           v
┌─────────────────────────────────────────────────────────┐
│               Background I/O Thread                     │
│  - Runs asyncio loop                                    │
│  - Batch-drains queue                                   │
│  - Handles socket I/O (Kafka / Object Storage)          │
└─────────────────────────────────────────────────────────┘
```

1. **SimPy Thread**: Executes SimPy processes and yields timeouts. When a process publishes an event or telemetry, the record is appended to a buffer for each egress provider. A buffer is handed to that provider's thread-safe queue when it reaches `batch_size`, or when `flush_interval` passes on a paced run.
2. **Background Asyncio Thread**: Automatically spawned on simulation startup. It drains the queues, and handles socket-level communication asynchronously, so socket latency does not stall each simulation step. Ingress providers run on a second background thread of their own. The egress queues are bounded by `max_queued_batches`, so a sink that cannot keep up slows the simulation rather than building a backlog, and a sink that stops consuming raises an error once the queue has been full for `drain_stall_seconds`.
