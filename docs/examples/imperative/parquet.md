# Fast-Forward to Parquet (Low-Level Imperative API)

While `dynamic-des` is designed for real-time digital twins, it is equally powerful as a **synchronized forecasting engine**. By manipulating the environment's time factor and initial state, you can run simulations to generate vast amounts of historical data or instantly predict future states.

This example demonstrates how to run a simulation in **fast-forward mode** using the low-level **Imperative API** and write compressed columnar data (Parquet) directly to local storage or an AWS S3 data lake using the `ParquetStorageEgress` connector.

---

## Quick Start

Download the script, then run it. The run writes Parquet chunks to a local `data/` folder by default, so no infrastructure is needed.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/imperative/parquet_example.py
```

### With uv

```bash
# 1. Run the simulation
uv run --no-project --with "dynamic-des[parquet]" parquet_example.py
```

### With pip

```bash
# 1. Install the package with the parquet extra
pip install "dynamic-des[parquet]"

# 2. Run the simulation
python parquet_example.py
```

To write to S3 instead, start the object store and set `USE_S3`. The chunks land under the `odctl-dev/history/` prefix, browsable at <http://localhost:8889>. `odctl` comes from `uv tool install "odctl>=0.5.1"` or `pip install "odctl>=0.5.1"`.

```bash
# 1. Spin up SeaweedFS with odctl
odctl up storage

# 2. Run the simulation against S3, with uv
USE_S3=true uv run --no-project --with "dynamic-des[parquet]" parquet_example.py

#    ...or with pip
USE_S3=true python parquet_example.py

# 3. Clean up the infrastructure when finished
odctl down storage --volumes
```

`DEST_PATH`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` override the destination and credentials.

## Full Source Code

This script simulates a manufacturing line over a 7-day period. It demonstrates how to route lifecycle events to one Parquet dataset, drop real-time metrics, and write them out.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

```python title="examples/imperative/parquet_example.py"
"""Historical data generation, imperative API.

The low-level twin of `declarative/parquet_example.py`. It wires
`DynamicRealtimeEnvironment`, the registry and the connectors by hand rather than
through the builder, which shows what `SimulationContext` does for you.

`factor=0.0` detaches the clock from real time, so seven days of history are generated
as fast as the machine allows and written to Parquet. A router keeps lifecycle events
and drops telemetry. Writes to `data/` unless `USE_S3=true`.
"""

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
logger = logging.getLogger("parquet_example")


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
    # odctl-dev is one of the buckets the odctl `storage` profile creates.
    base_path = os.getenv("DEST_PATH", "odctl-dev/history" if use_s3 else "data")
    filesystem = None

    if use_s3:
        # Lazy import PyArrow so the script doesn't crash if running purely local
        # without the [parquet] extra installed (though it is needed for ParquetEgress)
        from pyarrow import fs

        logger.info(f"Configuring S3 Egress. Target Bucket: '{base_path}'")
        filesystem = fs.S3FileSystem(
            access_key=os.getenv("S3_ACCESS_KEY", "user"),
            secret_key=os.getenv("S3_SECRET_KEY", "password"),
            endpoint_override=os.getenv("S3_ENDPOINT", "127.0.0.1:8333"),
            scheme="http",
        )
        # Ensure S3 Bucket exists
        filesystem.create_dir(base_path)
    else:
        logger.info(f"Configuring Local Egress. Target Folder: '{base_path}'")
        # Ensure local directory exists
        os.makedirs(base_path, exist_ok=True)

    # ---------------------------------------------------------
    # 2. SIMULATION SETUP
    # ---------------------------------------------------------
    line_a_params = SimParameter(
        sim_id="Line_A",
        arrival={"standard": DistributionConfig(dist="exponential", rate=2.0)},
        service={"milling": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
        resources={"lathe": CapacityConfig(current_cap=4, max_cap=10)},
    )

    start_time = datetime.now() - timedelta(days=7)
    env = DynamicRealtimeEnvironment(factor=0.0, logical_start_time=start_time)
    env.registry.register_sim_parameter(line_a_params)

    # Initialize Egress with the dynamic router and conditional filesystem
    router = create_history_router(base_path)
    egress = ParquetStorageEgress(path_router=router, filesystem=filesystem)

    # batch_size=5000 for compression, flush_interval=86400 (1 day in sim time)
    env.setup_egress([egress], batch_size=5000, flush_interval=86400)

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

    def telemetry_monitor(env: DynamicRealtimeEnvironment, res: DynamicResource):
        while True:
            env.publish_telemetry("Line_A.lathe.capacity", res.capacity)
            env.publish_telemetry("Line_A.lathe.in_use", res.in_use)
            env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))

            util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
            env.publish_telemetry("Line_A.lathe.utilization", util)

            yield env.timeout(60.0)

    env.process(arrival_process(env, res))
    env.process(telemetry_monitor(env, res))

    run_duration_str = "1 week"
    run_duration_sec = time_to_seconds(run_duration_str)

    logger.info(
        f"Generating historical data from {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("Fast-forwarding (factor=0.0)...")

    try:
        env.run(until=run_duration_sec)
    finally:
        env.teardown()
        logger.info(f"Data generation complete. Check '{base_path}/' for chunks.")


if __name__ == "__main__":
    run()
```
