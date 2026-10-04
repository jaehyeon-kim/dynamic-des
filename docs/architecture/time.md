# Time

A run has two clocks. Simulation time is the number of seconds SimPy has advanced, starting at 0. Logical time is the date and time each record carries, counted from `logical_start_time`. `factor` decides how simulation time relates to the wall clock, and `go_live_at` switches that relation once during the run.

The same arguments exist on `DynamicRealtimeEnvironment`, on `SimulationContext` and in the `simulation` section of a [YAML blueprint](yaml.md).

---

## Logical Start Time

`logical_start_time` is the date and time that simulation time 0 maps to. Without it, the run uses the moment the environment is created. Every record's `timestamp` is `logical_start_time` plus `sim_ts` seconds, as an ISO string with milliseconds, so a run with a backdated start writes history:

```python
from datetime import datetime, timedelta

from dynamic_des import SimulationContext

app = SimulationContext(
    sim_id="Line_A",
    factor=0.0,
    logical_start_time=datetime.now() - timedelta(days=7),
)
```

A naive datetime gives timestamps with no time zone. A timezone-aware one gives timestamps with its offset. [Records and Telemetry](records.md) shows where the two times appear in a record.

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

## What Happens at the Boundary

Pacing is driven by the logical clock, not by how long the process has been running. The switch is applied to the first event scheduled at or after `go_live_at`, before that event is paced, so no event is ever paced under the wrong factor.

At the switch the mapping from simulated time to wall-clock time is re-anchored: the go-live instant is treated as now, and simulated seconds run from there. Without that, the first paced event would sleep off the whole backfill in real seconds, and a week of history would become a week of waiting.

The factor switched to is always `1.0`. `go_live_at` names a moment, not a speed, and going live means real time. No other change of pacing can be scheduled. A [YAML scenario](../guides/yaml-blueprints.md#2-script-an-experiment), from [issue #15](https://github.com/jaehyeon-kim/dynamic-des/issues/15), schedules changes to registry values at set simulation times, but not to the pacing.

Three cases are worth stating explicitly.

* **`go_live_at` before `logical_start_time`**: the whole run is paced in real time, which is the same rule applied to an instant already past. A warning naming both instants is logged, because this is usually a mistake in the arithmetic that produced them.
* **`go_live_at` in the past but after `logical_start_time`**: the normal backfill case, and also what you get if the history takes a while to generate. The logical clock keeps whatever offset from the wall clock it had at the switch, and holds it for the rest of the run. Simulated seconds pass at one per real second, but the timestamps stay behind the wall clock by that offset.
* **A run that ends before `go_live_at`**: nothing happens, the run stays unpaced and ends as it would have. `go_live_at` schedules no event of its own, so it never holds a finished simulation open.

`go_live_at` is read against the same clock as `logical_start_time`, so both must be naive datetimes or both timezone-aware. Mixing them raises a `ValueError` when the environment is built, rather than failing later: at construction for `DynamicRealtimeEnvironment`, and at `run()` for `SimulationContext`. A YAML blueprint is rejected when the file is loaded, with the line of `go_live_at`.

---

## What `factor=0` Changes

A run at `factor=0.0` has no relation to the wall clock until `go_live_at`, if one is set. Three things behave differently:

* **The `flush_interval` timer is not started**, because a timer in simulation seconds would fire constantly. Records leave each buffer only when it reaches `batch_size`, and at teardown. [Batching and Delivery](batching.md) explains both limits.
* **`LocalIngress` follows the wall clock**, so its changes can land after the run has ended. A scenario follows the simulation clock and works at any factor. [Scenarios versus `LocalIngress`](registry.md#scenarios-versus-localingress) compares the two.
* **`system.simulation.lag_seconds`** is the wall-clock time since `logical_start_time` minus the simulation time, and never less than 0. An unpaced run stays ahead of the wall clock, so it reports 0 unless its start is backdated.

---

## Run Length

`until` is in simulation seconds. `SimulationContext.run()` and a blueprint's `run.until` also take a duration string such as `"10 min"`, `"8 hours"` or `"1 week"`, which `time_to_seconds` converts. A month counts as 30 days and a year as 365 days. Without `until`, SimPy runs until no events remain, so a run with an arrival loop continues until it is stopped.
