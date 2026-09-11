# Fast-Forward to Data Lake (Standard Declarative API)

While `dynamic-des` is designed for real-time digital twins, it is equally powerful as a **synchronized forecasting engine**. By manipulating the environment's time factor and initial state, you can run simulations to generate vast amounts of historical data or instantly predict future states.

This example demonstrates how to run a simulation in **fast-forward mode** using the declarative **Standard API (`SimulationContext`)** and write compressed columnar data (Parquet) directly to local storage or an AWS S3 data lake using the `ParquetStorageEgress` connector.

---

## Quick Start

Download the script, then run it. The run writes Parquet chunks to a local `data/` folder by default, so no infrastructure is needed.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/declarative/history_example.py
```

### With uv

```bash
# 1. Run the simulation
uv run --no-project --with "dynamic-des[parquet]" history_example.py
```

### With pip

```bash
# 1. Install the package with the parquet extra
pip install "dynamic-des[parquet]"

# 2. Run the simulation
python history_example.py
```

To write to S3 instead, start the object store and set `USE_S3`. The chunks land under the `odctl-dev/history/` prefix, browsable at <http://localhost:8889>. `odctl` comes from `uv tool install "odctl>=0.5.1"` or `pip install "odctl>=0.5.1"`.

```bash
# 1. Spin up SeaweedFS with odctl
odctl up storage

# 2. Run the simulation against S3, with uv
USE_S3=true uv run --no-project --with "dynamic-des[parquet]" history_example.py

#    ...or with pip
USE_S3=true python history_example.py

# 3. Clean up the infrastructure when finished
odctl down storage --volumes
```

`DEST_PATH`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` override the destination and credentials.

## Full Source Code

This script simulates a manufacturing line over a 7-day period. It demonstrates how to route lifecycle events to one Parquet dataset, drop real-time metrics, and write them out instantly.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

```python title="examples/declarative/history_example.py"
"""
Historical Data Generation Example.

Demonstrates using SimulationContext as a fast-forward data engine.
By setting `factor=0.0`, the SimPy clock detaches from wall-clock time,
executing the exact same factory logic instantly to generate massive
historical datasets for Machine Learning models via Parquet/S3.
"""

import logging
import os
from datetime import datetime, timedelta

from dynamic_des import ParquetStorageEgress, SimulationContext

# Logging is configured here rather than in a wrapper, because this script is run
# directly. Without it the run produces no output at all.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


def create_history_router(base_path: str):
    """
    Router Factory: Generates a router function injected with the correct
    base path, and flattens nested event payloads for Parquet.
    """

    def history_router(data: dict) -> str | None:
        if data.get("path_id") == "system.simulation.lag_seconds":
            return None

        stream_type = data.get("stream_type")

        if stream_type == "telemetry":
            return None

        # FLATTEN EVENT FOR PARQUET
        if (
            stream_type == "event"
            and "value" in data
            and isinstance(data["value"], dict)
        ):
            nested_value = data.pop("value")
            data.update(nested_value)

        return f"{base_path}/events.parquet"

    return history_router


# ==========================================
# 1. DUAL-MODE STORAGE CONFIGURATION
# ==========================================
# Reading environment variables and constructing the S3 client are safe at import.
# Creating the destination is not, so it lives in ensure_destination() and runs from
# run(). Importing this module for discovery, by a test collector, a docs build or the
# entry-point wiring in examples/__init__.py, must not create a directory or reach out
# to S3.
use_s3 = os.getenv("USE_S3", "false").lower() == "true"
# odctl-dev is one of the buckets the odctl `storage` profile creates.
base_path = os.getenv("DEST_PATH", "odctl-dev/history" if use_s3 else "data")
filesystem = None

if use_s3:
    from pyarrow import fs

    filesystem = fs.S3FileSystem(
        access_key=os.getenv("S3_ACCESS_KEY", "user"),
        secret_key=os.getenv("S3_SECRET_KEY", "password"),
        endpoint_override=os.getenv("S3_ENDPOINT", "127.0.0.1:8333"),
        scheme="http",
    )


def ensure_destination() -> None:
    """Create the target directory or bucket path, once the caller has asked to run."""
    if use_s3 and filesystem is not None:
        logger.info(f"Configuring S3 Egress. Target Bucket: '{base_path}'")
        filesystem.create_dir(base_path)
    else:
        logger.info(f"Configuring Local Egress. Target Folder: '{base_path}'")
        os.makedirs(base_path, exist_ok=True)


router = create_history_router(base_path)

# The week of backdating this example exists to demonstrate. It sits at module scope
# because the builder below needs it, and the builder has to stay at module scope for
# the decorators further down to attach to it. The imperative twin computes the same
# value inside run(), where it has no such constraint.
LOGICAL_START_TIME = datetime.now() - timedelta(days=7)

# ==========================================
# 2. Declarative Infrastructure Builder
# ==========================================
app = (
    SimulationContext(
        sim_id="Line_A",
        factor=0.0,
        random_seed=42,
        # Without this the example logs that it is backdating a week and then timestamps
        # every record from the current clock, which is the opposite of what it claims.
        logical_start_time=LOGICAL_START_TIME,
    )
    .add_egress(ParquetStorageEgress(path_router=router, filesystem=filesystem))
    .with_batching(batch_size=5000, flush_interval=86400)
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
    Returns the exact flat dictionary expected by the parquet router
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


@app.telemetry_loop(interval=60.0)
def telemetry_generator(context):
    """Samples the hidden state of the resources every 60 simulation seconds."""
    res = context.get_resource("lathe")

    context.publish("lathe.capacity", res.capacity)
    context.publish("lathe.in_use", res.in_use)
    context.publish("lathe.queue_length", len(res.queue.items))

    util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
    context.publish("lathe.utilization", util)


# ==========================================
# 4. Execution
# ==========================================
def run():
    """Generates 1 week of factory data instantly."""
    ensure_destination()

    logger.info(
        "Generating historical data mimicking start from "
        f"{LOGICAL_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("Fast-forwarding (factor=0.0)...")

    # The clock detaches and executes 1 week of operations instantaneously
    app.run(until="1 week")

    logger.info(f"Data generation complete. Check '{base_path}/' for chunks.")


if __name__ == "__main__":
    run()
```
