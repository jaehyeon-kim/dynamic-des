# Fast-Forward to Iceberg (Low-Level Imperative API)

`ParquetStorageEgress` writes files and leaves registering them as a table to you. This connector appends into an Iceberg table instead, so one run ends with something another engine can query.

This example demonstrates how to run a simulation in **fast-forward mode** using the low-level **Imperative API (`DynamicRealtimeEnvironment`)** and commit the output straight into an Apache Iceberg table using the `IcebergStorageEgress` connector.

Each flush of the buffer is one Iceberg commit. Every commit writes a manifest, a manifest list and a new `metadata.json`, so query planning degrades as snapshots accumulate. That is why this example gives the provider a large `batch_size` of its own: a day of events lands in four snapshots rather than eighty-five.

---

## Quick Start

The catalog is infrastructure, so unlike the Parquet example this one needs a container. `odctl` comes from `uv tool install "odctl>=0.5.1"` or `pip install "odctl>=0.5.1"`. The `catalog` profile brings up the Iceberg REST catalog backed by Postgres, and SeaweedFS for the data files.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/imperative/iceberg_example.py
```

```bash
# 1. Spin up the Iceberg REST catalog with odctl
odctl up catalog

# 2. Run the simulation, with uv
uv run --no-project --with "dynamic-des[iceberg]" iceberg_example.py

#    ...or with pip, after `pip install "dynamic-des[iceberg]"`
python iceberg_example.py

# 3. Clean up the infrastructure when finished
odctl down catalog --volumes
```

`ICEBERG_URI`, `ICEBERG_WAREHOUSE`, `ICEBERG_NAMESPACE`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` override the catalog and credentials.

Running it twice appends to the same table rather than replacing it, which is what an Iceberg table is for. Use `odctl down catalog --volumes` to start from nothing.

## Full Source Code

This script simulates a manufacturing line over a 1-day period. It routes lifecycle events into one Iceberg table, drops telemetry, and pins the schema so the event time is a real timestamp rather than the string inference would give.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

```python title="examples/imperative/iceberg_example.py"
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
```
