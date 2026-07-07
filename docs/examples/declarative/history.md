# Fast-Forward to Data Lake (Standard Declarative API)

While `dynamic-des` is designed for real-time digital twins, it is equally powerful as a **synchronized forecasting engine**. By manipulating the environment's time factor and initial state, you can run simulations to generate vast amounts of historical data or instantly predict future states.

This example demonstrates how to run a simulation in **fast-forward mode** using the declarative **Standard API (`SimulationContext`)** and write compressed columnar data (Parquet) directly to local storage or an AWS S3 data lake using the `ParquetStorageEgress` connector.

---

## Code

This script simulates a manufacturing line over a 7-day period. It demonstrates how to route lifecycle events to one Parquet dataset, drop real-time metrics, and write them out instantly.

```python
import logging
import os
from datetime import datetime, timedelta
from dynamic_des import SimulationContext, ParquetStorageEgress
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

        # Flatten Event for Parquet
        if (
            stream_type == "event"
            and "value" in data
            and isinstance(data["value"], dict)
        ):
            # Extract and remove the nested 'value' object
            nested_value = data.pop("value")
            # Merge the nested keys directly into the root dict
            data.update(nested_value)

        return f"{base_path}/events.parquet"

    return history_router

def run():
    # 1. Dual-Mode Storage Configuration (Local or S3)
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

    # 2. Setup Egress with routing
    router = create_history_router(base_path)
    egress = ParquetStorageEgress(
        path_router=router,
        filesystem=filesystem,
        batch_size=5000,  # Large batches for efficient disk writes
    )

    # 3. Define historical start point (e.g. 7 days ago)
    start_time = datetime.now() - timedelta(days=7)
    duration_seconds = time_to_seconds("7d")

    # 4. Build context with fast-forward time factor (factor=0.0)
    app = (
        SimulationContext(
            sim_id="Line_A",
            factor=0.0,  # Fast-forward mode (instant execution)
            random_seed=42,
            logical_start_time=start_time,
        )
        .add_resource("lathe", current_cap=2)
        .add_arrival("standard", dist="exponential", rate=0.2)
        .add_service("milling", dist="normal", mean=4.0, std=0.5)
        .add_egress(egress)
    )

    # 5. Define simulation processes using decorators
    @app.arrival_loop("standard")
    def arrival_process(context: SimulationContext):
        task_id = 0
        while True:
            yield context.wait_for_arrival("standard")
            context.spawn(work_task(task_id))
            task_id += 1

    @app.task(service_id="milling", resource_id="lathe")
    def work_task(task_id: int):
        return {"part_id": task_id}

    # 6. Run the Simulation
    print(f"Generating 7 days of history starting from {start_time}...")
    app.run(until=duration_seconds)
    print("Historical data generation complete!")

if __name__ == "__main__":
    run()
```
