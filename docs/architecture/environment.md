# Realtime Environment

At the heart of every simulation run is the `DynamicRealtimeEnvironment`, which extends SimPy's core environment. It is responsible for controlling the logical clock, managing simulated process tasks, and coordinating temporal synchronization.

---

## Temporal Factor

Dynamic DES decoupling is achieved by setting the `factor` parameter on the environment. This determines how simulated seconds relate to real-world wall-clock seconds:

$$\text{Simulated Duration} = \text{Wall-Clock Duration} \times \text{factor}$$

### 1. Real-Time Mode (`factor=1.0`)
When `factor=1.0` (or another positive float), the environment clock synchronizes with the system clock. If a simulated process calls `yield env.timeout(5.0)`, the simulation process will pause and block for exactly 5 seconds of real-world time.
* **Use Case**: Live digital twins feeding real-time metrics dashboards.

### 2. Fast-Forward / Batch Mode (`factor=0.0`)
When `factor=0.0` (the default), the environment operates at maximum CPU speed without matching the real-world clock. Simulated timeouts take 0.0 seconds of real-world time to execute.
* **Use Case**: Fast-forwarding historical backfills, batch forecasting, and executing integration tests instantly.

### 3. Both, in one run (`go_live_at`)
`factor` applies until the logical clock reaches `go_live_at`, and from that instant the run is paced at one simulated second per real second. With a backdated `logical_start_time` and `factor=0.0`, a single run generates the history as fast as the machine allows and then keeps going in real time.

```python
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
│  - Pushes raw events to Egress Queue                    │
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

1. **SimPy Thread**: Executes SimPy processes and yields timeouts. When a process publishes telemetry, the event is immediately pushed to a thread-safe in-memory queue.
2. **Background Asyncio Thread**: Automatically spawned on simulation startup. It drains the event queue, bundles them, and handles socket-level communication asynchronously. This guarantees that socket latency or network drops never freeze the simulation clock.
