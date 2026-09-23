"""
Lakehouse Data Generation Example.

The Iceberg twin of `declarative/parquet_example.py`. The same fast-forward engine,
but each flush of the buffer becomes one Iceberg commit rather than one Parquet file,
so the run ends with a table another engine can query instead of a folder someone
still has to register.

Every commit writes a manifest, a manifest list and a new `metadata.json`, so the
buffer is sized to produce a handful of snapshots rather than hundreds. That is what
`batch_size` on `add_egress` is for.

Requires the odctl `catalog` profile: `odctl up catalog`.
"""

import logging
import os
from datetime import datetime, timedelta

import pyarrow as pa

from dynamic_des import IcebergStorageEgress, SimulationContext

# Logging is configured here rather than in a wrapper, because this script is run
# directly. Without it the run produces no output at all.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "simulation")
EVENTS_TABLE = f"{NAMESPACE}.events"

# Pinned rather than inferred. Inference reads the ISO timestamp the environment
# writes as a string, which is not what a consumer of an event table expects.
EVENTS_SCHEMA = pa.schema(
    [
        ("sim_ts", pa.float64()),
        ("timestamp", pa.timestamp("us")),
        ("key", pa.string()),
        ("path_id", pa.string()),
        ("status", pa.string()),
    ]
)


def create_table_router(events_table: str):
    """
    Router Factory: Generates a router returning `namespace.table`, and reshapes
    each event into the pinned schema.
    """

    def table_router(data: dict) -> str | None:
        if data.get("path_id") == "system.simulation.lag_seconds":
            return None

        if data.get("stream_type") != "event":
            return None

        # FLATTEN EVENT INTO THE PINNED COLUMNS
        nested_value = data.pop("value", None)
        if isinstance(nested_value, dict):
            data.update(nested_value)

        # A pinned timestamp column takes a datetime. PyArrow rejects the ISO string
        # the environment writes, so the conversion belongs here.
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])

        return events_table

    return table_router


# ==========================================
# 1. CATALOG CONFIGURATION
# ==========================================
# Building the catalog client is safe at import: it opens no connection until a
# request is made. Creating the namespace is not, so the connector does that on its
# first batch, inside the run.
def build_catalog():
    """Connects to the Iceberg REST catalog from the odctl `catalog` profile."""
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "odctl",
        **{
            "uri": os.getenv("ICEBERG_URI", "http://localhost:8181"),
            "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:8333"),
            "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "user"),
            "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "password"),
            "s3.region": os.getenv("S3_REGION", "us-east-1"),
        },
    )


router = create_table_router(EVENTS_TABLE)

# The day of backdating this example exists to demonstrate. It sits at module scope
# because the builder below needs it, and the builder has to stay at module scope for
# the decorators further down to attach to it.
LOGICAL_START_TIME = datetime.now() - timedelta(days=1)

# ==========================================
# 2. Declarative Infrastructure Builder
# ==========================================
app = (
    SimulationContext(
        sim_id="Line_A",
        factor=0.0,
        random_seed=42,
        logical_start_time=LOGICAL_START_TIME,
    )
    .add_egress(
        IcebergStorageEgress(
            catalog=build_catalog(),
            table_router=router,
            schemas={EVENTS_TABLE: EVENTS_SCHEMA},
        ),
        # One commit per flush. Sized so a day of events lands in a few snapshots
        # rather than the eighty-five a 50,000 record buffer produces.
        batch_size=200_000,
    )
    .add_resource("lathe", current_cap=4, max_cap=10)
    .add_service("milling", dist="normal", mean=2.0, std=0.2)
    .add_arrival("standard", dist="exponential", rate=2.0)
)


# ==========================================
# 3. Simulation Logic
# ==========================================
@app.task(service_id="milling", resource_id="lathe")
def process_part(task_id: int, context):
    """
    Returns the exact flat dictionary expected by the table router
    to represent the 'finished' state of the lifecycle.
    """
    return {"path_id": "Line_A.service.milling", "status": "finished"}


@app.arrival_loop("standard")
def arrival_generator(context):
    task_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(process_part(task_id, context))
        task_id += 1


# ==========================================
# 4. Execution
# ==========================================
def run():
    """Generates 1 day of factory data instantly, into an Iceberg table."""
    logger.info(
        "Generating historical data mimicking start from "
        f"{LOGICAL_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("Fast-forwarding (factor=0.0)...")

    # The clock detaches and executes 1 day of operations instantaneously
    app.run(until="1 day")

    table = build_catalog().load_table(EVENTS_TABLE)
    logger.info(
        f"Wrote {table.scan().to_arrow().num_rows} rows to '{EVENTS_TABLE}' "
        f"in {len(table.metadata.snapshots)} snapshots at {table.location()}"
    )


if __name__ == "__main__":
    run()
