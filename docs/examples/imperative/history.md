# Fast-Forward to Data Lake (Low-Level Imperative API)

While `dynamic-des` is designed for real-time digital twins, it is equally powerful as a **synchronized forecasting engine**. By manipulating the environment's time factor and initial state, you can run simulations to generate vast amounts of historical data or instantly predict future states.

This example demonstrates how to run a simulation in **fast-forward mode** using the low-level **Imperative API** and write compressed columnar data (Parquet) directly to local storage or an AWS S3 data lake using the `ParquetStorageEgress` connector.

---

## Code

This script simulates a manufacturing line over a 7-day period. It demonstrates how to route lifecycle events to one Parquet dataset, drop real-time metrics, and write them out.

```python
import logging
import os
from datetime import datetime, timedelta

import numpy as np

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    ParquetStorageEgress,
    Sampler,
    SimParameter,
)
from dynamic_des.utils import time_to_seconds

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
logger = logging.getLogger("history_example")


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
        # If it's an event and has a nested 'value' dictionary, flatten it
        if (
            stream_type == "event"
            and "value" in data
            and isinstance(data["value"], dict)
        ):
            # Extract and remove the nested 'value' object
            nested_value = data.pop("value")
            # Merge the nested keys (path_id, status) directly into the root dict
            data.update(nested_value)

        return f"{base_path}/events.parquet"

    return history_router


def run():
    # ---------------------------------------------------------
    # 1. DUAL-MODE STORAGE CONFIGURATION
    # ---------------------------------------------------------
    use_s3 = os.getenv("USE_S3", "false").lower() == "true"
    base_path = os.getenv("DEST_PATH", "dml-dev/history" if use_s3 else "data")
    filesystem = None

    if use_s3:
        from pyarrow import fs

        filesystem = fs.S3FileSystem(
            endpoint_override=os.getenv("S3_ENDPOINT", "localhost:8333"),
            access_key=os.getenv("S3_ACCESS_KEY", "user"),
            secret_key=os.getenv("S3_SECRET_KEY", "password"),
            scheme="http",
        )

    # ---------------------------------------------------------
    # 2. ROUTING CONFIGURATION
    # ---------------------------------------------------------
    router = create_history_router(base_path)
    egress = ParquetStorageEgress(
        path_router=router,
        filesystem=filesystem,
        batch_size=5000,  # Large batches are best for generating Parquet chunks
    )

    # ---------------------------------------------------------
    # 3. INITIAL STATE & TIME OVERRIDES
    # ---------------------------------------------------------
    # Backdate simulation to start 7 days ago
    start_time = datetime.now() - timedelta(days=7)
    duration_seconds = time_to_seconds("7d")

    line_a_params = SimParameter(
        sim_id="Line_A",
        arrival={"standard": DistributionConfig(dist="exponential", rate=0.2)},
        service={"milling": DistributionConfig(dist="normal", mean=4.0, std=0.5)},
        resources={"lathe": CapacityConfig(current_cap=2, max_cap=5)},
    )

    # ---------------------------------------------------------
    # 4. INSTANTIATION
    # ---------------------------------------------------------
    # factor=0.0 runs simulation instantly (no wall-clock sync)
    env = DynamicRealtimeEnvironment(factor=0.0, logical_start_time=start_time)
    env.registry.register_sim_parameter(line_a_params)
    env.setup_egress([egress])

    res = DynamicResource(env, "Line_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(42))

    # ---------------------------------------------------------
    # 5. SIMULATION LOGIC
    # ---------------------------------------------------------
    def arrival_process(env, res):
        arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(arrival_cfg))
            env.process(work_task(env, task_id, res, "Line_A.service.milling"))
            task_id += 1

    def work_task(env, task_id, res, path_id):
        task_key = f"task-{task_id}"
        env.publish_event(task_key, {"path_id": path_id, "status": "queued"})

        with res.request() as req:
            yield req
            current_service_cfg = env.registry.get_config(path_id)
            env.publish_event(task_key, {"path_id": path_id, "status": "started"})
            yield env.timeout(sampler.sample(current_service_cfg))
            env.publish_event(task_key, {"path_id": path_id, "status": "finished"})

    # ---------------------------------------------------------
    # 6. EXECUTION
    # ---------------------------------------------------------
    env.process(arrival_process(env, res))

    print(f"Generating 7 days of history starting from {start_time}...")
    try:
        env.run(until=duration_seconds)
    finally:
        env.teardown()

    print("Historical data generation complete!")


if __name__ == "__main__":
    run()
```
