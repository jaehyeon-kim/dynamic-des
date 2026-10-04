# Realtime Environment

At the heart of every simulation run is the `DynamicRealtimeEnvironment`, which extends SimPy's core environment. It is responsible for controlling the logical clock, managing simulated process tasks, and coordinating temporal synchronization.

---

## Temporal Factor

Dynamic DES decoupling is achieved by setting the `factor` parameter on the environment. This determines how simulated seconds relate to real-world wall-clock seconds:

```text
Wall-Clock Duration = Simulated Duration × factor
```

### 1. Real-Time Mode (`factor=1.0`)
When `factor=1.0` (or another positive float), the environment clock synchronizes with the system clock. If a simulated process calls `yield env.timeout(5.0)`, the simulation process will pause and block for 5 × `factor` seconds of real-world time, which is 5 seconds at `factor=1.0`.
* **Use Case**: Live digital twins feeding real-time metrics dashboards.

### 2. Fast-Forward / Batch Mode (`factor=0.0`)
When `factor=0.0`, the environment operates at maximum CPU speed without matching the real-world clock. Simulated timeouts take 0.0 seconds of real-world time to execute.
* **Use Case**: Fast-forwarding historical backfills, batch forecasting, and executing integration tests instantly.

### 3. Both, in one run (`go_live_at`)
`factor` applies until the logical clock reaches `go_live_at`, and from that instant the run is paced at one simulated second per real second. With a backdated `logical_start_time` and `factor=0.0`, a single run generates the history as fast as the machine allows and then keeps going in real time.

```python
from datetime import datetime, timedelta

from dynamic_des import DynamicRealtimeEnvironment

go_live_at = datetime.now()

env = DynamicRealtimeEnvironment(
    factor=0.0,
    logical_start_time=go_live_at - timedelta(days=7),
    go_live_at=go_live_at,
)
```

* **Use Case**: Seeding a lake with history and then feeding a live stream, without a second process.
* An instant at or before `logical_start_time` paces the whole run, and a run that ends first is left unpaced. Both datetimes must be naive, or both timezone-aware.
* See [Backfill Then Go Live in One Run](../guides/backfill-then-live.md) for the full pattern, including how to route the two halves to different sinks.

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
