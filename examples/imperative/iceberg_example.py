"""Lakehouse data generation, imperative API.

The low-level twin of `declarative/iceberg_example.py`. It wires
`DynamicRealtimeEnvironment`, the registry and the connectors by hand rather than
through the builder, which shows what `SimulationContext` does for you.

`factor=0.0` detaches the clock from real time, so a day of history is generated as
fast as the machine allows and committed into an Iceberg table. Each flush of the
buffer is one commit, so `batch_sizes` is what keeps the snapshot count down.

Requires the odctl `catalog` profile: `odctl up catalog`.
"""

import logging
import os
from datetime import datetime, timedelta

import numpy as np
import pyarrow as pa

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    IcebergStorageEgress,
    Sampler,
    SimParameter,
)
from dynamic_des.utils import time_to_seconds

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
logger = logging.getLogger("iceberg_example")

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


def run():
    # ---------------------------------------------------------
    # 1. SIMULATION SETUP
    # ---------------------------------------------------------
    line_a_params = SimParameter(
        sim_id="Line_A",
        arrival={"standard": DistributionConfig(dist="exponential", rate=2.0)},
        service={"milling": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
        resources={"lathe": CapacityConfig(current_cap=4, max_cap=10)},
    )

    start_time = datetime.now() - timedelta(days=1)
    env = DynamicRealtimeEnvironment(factor=0.0, logical_start_time=start_time)
    env.registry.register_sim_parameter(line_a_params)

    egress = IcebergStorageEgress(
        catalog=build_catalog(),
        table_router=create_table_router(EVENTS_TABLE),
        schemas={EVENTS_TABLE: EVENTS_SCHEMA},
    )

    # One commit per flush, so a day of events lands in a handful of snapshots.
    env.setup_egress([egress], batch_sizes=[200_000], flush_interval=86400)

    res = DynamicResource(env, "Line_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(42))

    def arrival_process(env: DynamicRealtimeEnvironment, res: DynamicResource):
        arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
        service_path = "Line_A.service.milling"
        task_id = 0

        while True:
            yield env.timeout(sampler.sample(arrival_cfg))
            env.process(work_task(env, task_id, res, service_path))
            task_id += 1

    def work_task(
        env: DynamicRealtimeEnvironment,
        task_id: int,
        res: DynamicResource,
        path_id: str,
    ):
        task_key = f"task-{task_id}"
        env.publish_event(task_key, {"path_id": path_id, "status": "queued"})

        with res.request() as req:
            yield req
            current_service_cfg = env.registry.get_config(path_id)
            env.publish_event(task_key, {"path_id": path_id, "status": "started"})
            yield env.timeout(sampler.sample(current_service_cfg))
            env.publish_event(task_key, {"path_id": path_id, "status": "finished"})

    env.process(arrival_process(env, res))

    run_duration_str = "1 day"
    run_duration_sec = time_to_seconds(run_duration_str)

    logger.info(
        f"Generating historical data from {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("Fast-forwarding (factor=0.0)...")

    try:
        env.run(until=run_duration_sec)
    finally:
        env.teardown()

    table = build_catalog().load_table(EVENTS_TABLE)
    logger.info(
        f"Wrote {table.scan().to_arrow().num_rows} rows to '{EVENTS_TABLE}' "
        f"in {len(table.metadata.snapshots)} snapshots at {table.location()}"
    )


if __name__ == "__main__":
    run()
