# Backfill Then Go Live in One Run

A tiered demo usually needs two datasets: a backdated history sitting in a lake, and a live stream arriving now. They have to be the same factory, with the same machines, the same distributions and the same task ids, or the join between them is meaningless.

Producing them takes two settings that contradict each other. History wants `factor=0.0`, so a week of events is generated as fast as the machine allows. A live tail wants `factor=1.0`, so events arrive one simulated second per real second.

---

## Why one run rather than two

Before `go_live_at`, the only way to get both was to start two processes with the same `random_seed` and the same `logical_start_time`, one fast-forwarding into Parquet and one pacing into Kafka. That is what the `architecting-analytics-clickhouse-iceberg` bootcamp does, and it costs:

* A second full execution of the same simulation, so the history is generated twice.
* Seed and start instant duplicated in two places, which have to be edited together. Change one and the two halves silently stop being the same factory.
* No guarantee about the seam. Each process decides on its own where its half ends, so the join can overlap or leave a gap.

`go_live_at` replaces both processes with one. The run generates history unpaced up to that logical instant, then switches to real-time pacing and keeps going. There is one seed, one start instant and one seam.

---

## How the three settings fit together

Three separate settings make up the feature, and each answers a different question.

| Setting | Question it answers |
| --- | --- |
| `logical_start_time` | Where does the logical clock start? Set it in the past to backdate the history. |
| `go_live_at` | At which logical instant does pacing stop being `factor` and become real time? |
| `when` on `add_egress` | Which sink receives each record? |

They are independent. `go_live_at` changes pacing only; it never routes a record. `when` routes records only; it never changes pacing. Using them together is what produces a tiered dataset, and the two instants have to agree: pass the same instant to `go_live_at` and to the predicates, or the seam in the data will not match the seam in the pacing.

```text
  logical_start_time                 go_live_at                      end of run
        │                                 │                               │
        │   factor=0.0, no real time      │   1 sim second = 1 real second│
        ├─────────────────────────────────┼───────────────────────────────┤
        │        when=is_history          │          when=is_live         │
        v                                 v                               v
   ┌──────────────────────────────────────┐  ┌────────────────────────────┐
   │        Parquet (cold history)        │  │       Kafka (hot tail)     │
   └──────────────────────────────────────┘  └────────────────────────────┘
```

---

## Worked example

This run backdates the clock by ten minutes, writes those ten minutes to Parquet in under a second, then publishes to Kafka in real time for a minute. Set `HISTORY_MINUTES` and `LIVE_SECONDS` to change the two halves.

```python title="backfill_live_example.py"
--8<-- "src/dynamic_des/examples/declarative/backfill_live_example.py"
```

Two details in that script are easy to get wrong.

**Predicates compare strings, not datetimes.** A record carries its logical time as an ISO string, so `record["timestamp"] >= GO_LIVE_AT` raises a `TypeError`. Format the instant once with `isoformat(timespec="milliseconds")` and compare against that. Every timestamp is produced by the same formatter, so ordering by text is ordering by time.

**`flush_interval` is measured in simulated seconds.** It therefore means two different things in the two halves of the run. During the backfill it is instant, and it decides how many Parquet chunk files you get, because each flush writes one. Once live it is real seconds, and it decides how far behind the Kafka tail runs. A value of a few seconds suits both; a value of a day, as used for a pure history run, would hold the live tail in memory until teardown.

---

## Running it

```bash
# 1. Start Kafka
uv run ddes-kafka-infra-up

# 2. Ten minutes of history to Parquet, then a minute of live tail to Kafka
uv run ddes-backfill-live

# 3. Clean up
uv run ddes-kafka-infra-down
```

The history lands in `data/backfill/` as Parquet chunks, and the tail lands in the `sim-events` and `sim-telemetry` topics.

---

## What the run looks like while it happens

The two halves look so different that the second one can be mistaken for a hang. This is a shortened run, two minutes of history and twenty seconds of tail, so that both halves fit in one listing:

```bash
HISTORY_MINUTES=2 LIVE_SECONDS=20 uv run ddes-backfill-live
```

```text
18:02:24 Backfilling from 18:00:22 to 18:02:22 into 'data/backfill/', ...
18:02:24 Building SimulationContext for 'Line_A'...
18:02:24 Simulation engine started.
18:02:24 Logical clock reached go-live at 2026-09-11T18:02:22.879; pacing switched to real time.
18:02:24 Parquet Writer: Processed 10 batches. (~2 batches waiting in queue)
18:02:24 Kafka Egress producer connected successfully.
18:02:44 Environment teardown initiated.
```

Two minutes of simulated history are finished within the same second the run starts, and the go-live line follows immediately. After that the process spends twenty seconds apparently doing very little, because from that point it is waiting on the wall clock exactly as a live digital twin does. Log lines now appear at the rate events actually happen.

`Logical clock reached go-live` is the line that confirms the switch. It is logged once, by `dynamic_des.core.environment`, at `INFO`.

---

## What happens at the boundary

Pacing is driven by the logical clock, not by how long the process has been running. The switch is applied to the first event scheduled at or after `go_live_at`, before that event is paced, so no event is ever paced under the wrong factor.

At the switch the mapping from simulated time to wall-clock time is re-anchored: the go-live instant is treated as now, and simulated seconds run from there. Without that, the first paced event would sleep off the whole backfill in real seconds, and a week of history would become a week of waiting.

The factor switched to is always `1.0`. `go_live_at` names a moment, not a speed, and going live means real time. Scheduling an arbitrary change part way through a run is a separate feature, covered by [issue #15](https://github.com/jaehyeon-kim/dynamic-des/issues/15) on timed parameter mutations.

Three cases are worth stating explicitly.

* **`go_live_at` before `logical_start_time`**: the whole run is paced in real time, which is the same rule applied to an instant already past. A warning naming both instants is logged, because this is usually a mistake in the arithmetic that produced them.
* **`go_live_at` in the past but after `logical_start_time`**: the normal backfill case, and also what you get if the history takes a while to generate. The logical clock keeps whatever offset from the wall clock it had at the switch, and holds it for the rest of the run. Simulated seconds pass at one per real second, but the timestamps stay behind the wall clock by that offset.
* **A run that ends before `go_live_at`**: nothing happens, the run stays unpaced and ends as it would have. `go_live_at` schedules no event of its own, so it never holds a finished simulation open.

`go_live_at` is read against the same clock as `logical_start_time`, so both must be naive datetimes or both timezone-aware. Mixing them raises a `ValueError` at construction rather than failing later.

---

## Low-level API

`go_live_at` behaves identically on `DynamicRealtimeEnvironment`, alongside `logical_start_time`:

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

`env.factor` reports `0.0` until the logical clock reaches `go_live_at`, and `1.0` afterwards.
