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
