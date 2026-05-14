# Historical Data Generation

While `dynamic-des` is designed for real-time digital twins, it is equally powerful as a historical data generator. By manipulating the environment's time factor, you can simulate weeks, months, or years of events in a fraction of the actual time.

This is particularly useful for generating synthetic datasets to train Machine Learning models, backfilling Data Lakes, or testing downstream analytical pipelines.

## Key Concepts

* **Time Dilation (`factor=0.0`):** Setting the environment's factor to `0` tells the engine to execute events as fast as the CPU allows, completely detaching logical simulation time from real-world wall-clock time.
* **Storage Egress:** Instead of streaming to a live message broker like Kafka, historical generation typically writes directly to object storage (Local, S3, GCS) using the `ParquetStorageEgress` or `JsonlStorageEgress` connectors.
* **File Chunking:** To support massively parallel processing downstream (e.g., AWS Athena, Databricks), the storage connectors automatically chunk data into unique files (e.g., `events_a1b2c3d4.parquet`) based on your `batch_size`.
* **Dynamic Routing (Multiplexing):** You can provide a `path_router` function to dynamically split, route, or drop records on the fly.

## Execution

This example requires the `parquet` optional dependency to enable PyArrow's high-performance columnar writing capabilities. You can run the built-in demo directly from your terminal:

```bash
# Install with parquet support
pip install dynamic-des[parquet]

# Run the example
ddes-history-example
```

## Code

This script simulates a manufacturing line over a 1-week period. It demonstrates how to route lifecycle events to one Parquet dataset, drop meaningless real-time metrics (like system lag), and chunk the data perfectly for a Data Lake.

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


def history_router(data: dict) -> str | None:
    """Routes data to specific Parquet files and drops useless metrics."""
    # Drop lag_seconds entirely—meaningless in fast-forward mode
    if data.get("path_id") == "system.simulation.lag_seconds":
        return None 
        
    stream_type = data.get("stream_type")
    
    # Drop telemetry for this specific run, keeping only events
    if stream_type == "telemetry":
        return None
        
    return "data/events.parquet"


def run():
    # 1. Ensure our target directory exists
    os.makedirs("data", exist_ok=True)
    
    line_a_params = SimParameter(
        sim_id="Line_A",
        arrival={"standard": DistributionConfig(dist="exponential", rate=2.0)},
        service={"milling": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
        resources={"lathe": CapacityConfig(current_cap=4, max_cap=10)},
    )

    # 2. Setup Environment for Historical Fast-Forward
    # Start the simulation exactly 7 days ago
    start_time = datetime.now() - timedelta(days=7)
    
    # factor=0.0 runs the simulation as fast as possible
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        logical_start_time=start_time
    )
    env.registry.register_sim_parameter(line_a_params)

    # Initialize Egress with the custom router
    egress = ParquetStorageEgress(path_router=history_router)
    
    # batch_size=5000 ensures good Parquet compression.
    # flush_interval=86400 (1 day) prevents premature flushing in fast-forward mode.
    env.setup_egress([egress], batch_size=5000, flush_interval=86400)

    # 3. Initialize Resources and Sampler
    res = DynamicResource(env, "Line_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(42))

    # 4. Define Simulation Logic
    def arrival_process(env: DynamicRealtimeEnvironment, res: DynamicResource):
        arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
        service_path = "Line_A.service.milling"
        task_id = 0

        while True:
            yield env.timeout(sampler.sample(arrival_cfg))
            env.process(work_task(env, task_id, res, service_path))
            task_id += 1

    def work_task(env: DynamicRealtimeEnvironment, task_id: int, res: DynamicResource, path_id: str):
        task_key = f"task-{task_id}"
        env.publish_event(task_key, {"path_id": path_id, "status": "queued"})

        with res.request() as req:
            yield req
            current_service_cfg = env.registry.get_config(path_id)
            env.publish_event(task_key, {"path_id": path_id, "status": "started"})
            yield env.timeout(sampler.sample(current_service_cfg))
            env.publish_event(task_key, {"path_id": path_id, "status": "finished"})

    # 5. Run the Simulation
    env.process(arrival_process(env, res))

    # Run for exactly 1 logical week
    run_duration_str = "1 week"
    run_duration_sec = time_to_seconds(run_duration_str)
    
    logger.info(f"Generating historical data from {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("Fast-forwarding (factor=0.0)...")
    
    try:
        env.run(until=run_duration_sec)
    finally:
        # The teardown method safely drains the remaining queue to disk
        env.teardown()
        logger.info("Data generation complete. Check the 'data/' directory for chunks.")

if __name__ == "__main__":
    run()
```