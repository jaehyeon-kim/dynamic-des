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
  logical_start_time                 go_live_at                         end of run
        │                                 │                                  │
        │   factor=0.0, no real time      │   1 sim second = 1 real second.  │
        ├─────────────────────────────────┼──────────────────────────────────┤
        │        when=is_history          │           when=is_live           │
        v                                 v                                  v
   ┌──────────────────────────────────────┐  ┌────────────────────────────-──┐
   │        Parquet (cold history)        │  │       Kafka (hot tail)        │
   └──────────────────────────────────────┘  └───────────────────────────────┘
```

---

## Worked example

This run backdates the clock by ten minutes, writes those ten minutes to Parquet in well under a second, then publishes to Kafka in real time for sixty seconds. So the whole run takes about a minute, and nearly all of that is the live half. Set `HISTORY_MINUTES` and `LIVE_SECONDS` to change the two halves, for example `HISTORY_MINUTES=1440` for a day of history, which still generates in seconds: a day of it was measured at nine seconds end to end.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

```python title="examples/declarative/backfill_live_example.py"
"""
Backfill-then-live Example.

One run produces both halves of a tiered dataset. Until `go_live_at` the clock is
detached from the wall clock, so ten minutes of backdated history are written to
Parquet as fast as the machine allows. From `go_live_at` the same run is paced at one
simulated second per real second, and the same events are published to Kafka as they
happen. Doing this in two processes would mean repeating the seed and the start
instant in both, and keeping them in step by hand.
"""

import logging
import os
import time
from datetime import datetime, timedelta

from dynamic_des import (
    KafkaAdminConnector,
    KafkaEgress,
    ParquetStorageEgress,
    SimulationContext,
)

# Logging is configured here rather than in a wrapper, because this script is run
# directly. Without it the run produces no output at all.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENT_TOPIC = "sim-events"
TELEMETRY_TOPIC = "sim-telemetry"

# How much history to generate, and how long to keep tailing once live. The live half
# costs real time, second for second, so it is short by default.
HISTORY = timedelta(minutes=float(os.getenv("HISTORY_MINUTES", "10")))
LIVE_SECONDS = float(os.getenv("LIVE_SECONDS", "60"))

base_path = os.getenv("DEST_PATH", "data/backfill")

# The go-live instant is now, so everything before it is history and everything after
# it is the live tail. These sit at module scope because the builder below needs them,
# and the builder has to stay at module scope for the decorators to attach to it.
GO_LIVE_AT = datetime.now()
LOGICAL_START_TIME = GO_LIVE_AT - HISTORY

# Records carry their logical time as an ISO string, so the predicates that split the
# two sinks compare strings. That works because every timestamp comes from the same
# formatter: identical layout, so ordering by text is ordering by time.
GO_LIVE_ISO = GO_LIVE_AT.isoformat(timespec="milliseconds")


def is_history(record: dict) -> bool:
    """True for records stamped before the go-live instant."""
    return record["timestamp"] < GO_LIVE_ISO


def is_live(record: dict) -> bool:
    """True for records stamped at or after the go-live instant."""
    return record["timestamp"] >= GO_LIVE_ISO


def history_router(data: dict) -> str | None:
    """Drops telemetry and flattens the event payload, as Parquet needs flat rows."""
    if data.get("stream_type") != "event":
        return None

    if isinstance(data.get("value"), dict):
        data.update(data.pop("value"))

    return f"{base_path}/events.parquet"


# ==========================================
# 1. Declarative Infrastructure Builder
# ==========================================
app = (
    SimulationContext(
        sim_id="Line_A",
        # Unpaced to begin with, so the history costs no real time.
        factor=0.0,
        random_seed=42,
        logical_start_time=LOGICAL_START_TIME,
        # From here the same run is paced at one simulated second per real second.
        go_live_at=GO_LIVE_AT,
    )
    .add_egress(ParquetStorageEgress(path_router=history_router), when=is_history)
    .add_egress(
        KafkaEgress(
            event_topic=EVENT_TOPIC,
            telemetry_topic=TELEMETRY_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
        ),
        when=is_live,
    )
    # Only batch_size governs this run. The interval flush is a simulation process,
    # started only when factor is non-zero as the egress is set up, and this run starts
    # at 0.0. Records therefore leave the buffer when it fills to 2000, or at teardown.
    .with_batching(batch_size=2000, flush_interval=10.0)
    .add_resource("lathe", current_cap=4, max_cap=10)
    .add_service("milling", dist="normal", mean=2.0, std=0.2)
    .add_arrival("standard", dist="exponential", rate=0.5)
)


# ==========================================
# 2. Simulation Logic
# ==========================================
@app.task(service_id="milling", resource_id="lathe")
def process_part(task_id: int, context):
    """Returns the flat payload that both sinks receive for a finished task."""
    return {"path_id": "Line_A.service.milling", "status": "finished"}


@app.arrival_loop("standard")
def arrival_generator(context):
    task_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(process_part(task_id, context))
        task_id += 1


@app.telemetry_loop(interval=30.0)
def telemetry_generator(context):
    """Samples resource use every 30 simulation seconds."""
    res = context.get_resource("lathe")

    context.publish("lathe.in_use", res.in_use)
    context.publish("lathe.queue_length", len(res.queue.items))


# ==========================================
# 3. Execution
# ==========================================
def run():
    """Generates the history instantly, then tails live for LIVE_SECONDS."""
    os.makedirs(base_path, exist_ok=True)

    try:
        admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
        admin.create_topics(
            topics_config=[
                {"name": EVENT_TOPIC, "partitions": 1},
                {"name": TELEMETRY_TOPIC, "partitions": 1},
            ]
        )
        time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not explicitly create topics: {e}")

    logger.info(
        "Backfilling from %s to %s into '%s/', then tailing live to Kafka for %.0fs.",
        LOGICAL_START_TIME.strftime("%Y-%m-%d %H:%M:%S"),
        GO_LIVE_AT.strftime("%Y-%m-%d %H:%M:%S"),
        base_path,
        LIVE_SECONDS,
    )

    app.run(until=HISTORY.total_seconds() + LIVE_SECONDS)

    logger.info("Run complete. History is in '%s/', the tail is in Kafka.", base_path)


if __name__ == "__main__":
    run()
```

Two details in that script are easy to get wrong.

**Predicates compare strings, not datetimes.** A record carries its logical time as an ISO string, so `record["timestamp"] >= GO_LIVE_AT` raises a `TypeError`. Format the instant once with `isoformat(timespec="milliseconds")` and compare against that. Every timestamp is produced by the same formatter, so ordering by text is ordering by time.

**`flush_interval` has no effect in this run, so `batch_size` decides everything.** The interval flush is a simulation process, and it is only started if `factor` is non-zero when the egress is set up. This run starts at `factor=0.0`, and reaching `go_live_at` changes the pacing without starting that process. Records therefore leave the buffer only when it fills to `batch_size`, or at teardown. That is what sets the number of Parquet part files: a day of history is roughly 222,000 records, so `batch_size=2000` produced 111 of them. It also means the live half reaches Kafka in one go at teardown rather than every ten seconds, which was confirmed by watching the topic offsets stay still for the whole live half. Lower `batch_size` if you want either sink to receive data while the run is still going.

---

## Running it

Download the script, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/backfill_live_example.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Start Kafka
odctl up kafka-lite

# 3. Ten minutes of history to Parquet, then sixty seconds of live tail to Kafka
uv run --no-project --with "dynamic-des[kafka,parquet]" backfill_live_example.py

# 4. Clean up
odctl down kafka-lite --volumes
```

### With pip

```bash
# 1. Install the package with both extras, and odctl for the containers
pip install "dynamic-des[kafka,parquet]" "odctl>=0.5.1"

# 2. Start Kafka
odctl up kafka-lite

# 3. Ten minutes of history to Parquet, then sixty seconds of live tail to Kafka
python backfill_live_example.py

# 4. Clean up
odctl down kafka-lite --volumes
```

History lands in `data/backfill/` as Parquet part files named `events_<id>.parquet`, one per flush, so a ten minute history is a single file. The tail lands in the `sim-events` and `sim-telemetry` topics as the run tears down.

---

## What the run looks like while it happens

The two halves look so different that the second one can be mistaken for a hang. This is a shortened run, two minutes of history and twenty seconds of tail, so that both halves fit in one listing:

```bash
HISTORY_MINUTES=2 LIVE_SECONDS=20 uv run examples/declarative/backfill_live_example.py
```

Skipping the broker connection lines that `kafka-python` logs first, the run reads:

```text
19:05:23 [INFO] __main__: Backfilling from 2026-09-11 19:03:21 to 2026-09-11 19:05:21 into 'data/backfill/', then tailing live to Kafka for 20s.
19:05:23 [INFO] dynamic_des.core.context: Building SimulationContext for 'Line_A'...
19:05:23 [INFO] dynamic_des.core.context: Simulation engine started.
19:05:23 [INFO] dynamic_des.core.environment: Logical clock reached go-live at 2026-09-11T19:05:21.798; pacing switched to real time.
19:05:23 [INFO] dynamic_des.connectors.egress.kafka: Kafka Egress producer connected successfully.
19:05:43 [INFO] dynamic_des.core.environment: Environment teardown initiated.
19:05:43 [INFO] dynamic_des.core.environment: Tearing down egress connectors, flushing final events...
19:05:44 [INFO] dynamic_des.core.environment: Tearing down ingress connectors...
19:05:44 [INFO] dynamic_des.core.environment: Environment teardown complete.
19:05:44 [INFO] dynamic_des.connectors.egress.storage: ParquetStorageEgress shut down requested. Successfully wrote 1 total chunks to storage.
19:05:44 [INFO] __main__: Run complete. History is in 'data/backfill/', the tail is in Kafka.
```

Two minutes of simulated history are finished within the same second the run starts, and the go-live line follows immediately. After that the process spends twenty seconds apparently doing very little, because from that point it is waiting on the wall clock exactly as a live digital twin does. Nothing at all is logged during those twenty seconds, and the next lines are the teardown, which is where both sinks receive their data.

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
