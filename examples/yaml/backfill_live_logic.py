"""Python for examples/yaml/backfill_live.yaml: the instants, predicates and router."""

import logging
import os
import time
from datetime import datetime, timedelta

from dynamic_des import KafkaAdminConnector

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENT_TOPIC = "sim-events"
TELEMETRY_TOPIC = "sim-telemetry"

# How much history to generate, and how long to keep tailing once live. The live half
# costs real time, second for second, so it is short by default.
HISTORY = timedelta(minutes=float(os.getenv("HISTORY_MINUTES", "10")))
LIVE_SECONDS = float(os.getenv("LIVE_SECONDS", "60"))
UNTIL = HISTORY.total_seconds() + LIVE_SECONDS

base_path = os.getenv("DEST_PATH", "data/backfill")

# The go-live instant is now, so everything before it is history and everything after
# it is the live tail.
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


def prepare():
    """Creates the history folder and the Kafka topics before the run starts."""
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
