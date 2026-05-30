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
    base_path = os.getenv("DEST_PATH", "des-dev/history" if use_s3 else "data")
    filesystem = None

    if use_s3:
        # Lazy import PyArrow so the script doesn't crash if running purely local
        # without the [parquet] extra installed (though it is needed for ParquetEgress)
        from pyarrow import fs

        logger.info(f"Configuring S3 Egress. Target Bucket: '{base_path}'")
        filesystem = fs.S3FileSystem(
            access_key=os.getenv("S3_ACCESS_KEY", "user"),
            secret_key=os.getenv("S3_SECRET_KEY", "password"),
            endpoint_override=os.getenv("S3_ENDPOINT", "localhost:8333"),
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
