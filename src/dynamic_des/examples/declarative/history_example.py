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
use_s3 = os.getenv("USE_S3", "false").lower() == "true"
base_path = os.getenv("DEST_PATH", "des-dev/history" if use_s3 else "data")
filesystem = None

if use_s3:
    from pyarrow import fs

    logger.info(f"Configuring S3 Egress. Target Bucket: '{base_path}'")
    filesystem = fs.S3FileSystem(
        access_key=os.getenv("S3_ACCESS_KEY", "user"),
        secret_key=os.getenv("S3_SECRET_KEY", "password"),
        endpoint_override=os.getenv("S3_ENDPOINT", "localhost:8333"),
        scheme="http",
    )
    filesystem.create_dir(base_path)
else:
    logger.info(f"Configuring Local Egress. Target Folder: '{base_path}'")
    os.makedirs(base_path, exist_ok=True)

router = create_history_router(base_path)

# ==========================================
# 2. Declarative Infrastructure Builder
# ==========================================
app = (
    SimulationContext(sim_id="Line_A", factor=0.0, random_seed=42)
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
    start_time = datetime.now() - timedelta(days=7)
    logger.info(
        f"Generating historical data mimicking start from {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("Fast-forwarding (factor=0.0)...")

    # The clock detaches and executes 1 week of operations instantaneously
    app.run(until="1 week")

    logger.info(f"Data generation complete. Check '{base_path}/' for chunks.")


if __name__ == "__main__":
    run()
