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

# Assuming you added `time_to_seconds` to `utils.py` and exported it
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

    # Split into two separate data lakes
    if stream_type == "telemetry":
        return None

    return "data/events.parquet"


def run():
    os.makedirs("data", exist_ok=True)

    line_a_params = SimParameter(
        sim_id="Line_A",
        arrival={"standard": DistributionConfig(dist="exponential", rate=2.0)},
        service={"milling": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
        resources={"lathe": CapacityConfig(current_cap=4, max_cap=10)},
    )

    start_time = datetime.now() - timedelta(days=7)
    env = DynamicRealtimeEnvironment(factor=0.0, logical_start_time=start_time)
    env.registry.register_sim_parameter(line_a_params)

    # Initialize Egress with the router
    egress = ParquetStorageEgress(path_router=history_router)

    # batch_size=5000 for compression, flush_interval=86400 (1 day in sim time)
    # so we don't accidentally flush tiny files due to fast-forwarding!
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
        logger.info("Data generation complete. Check the 'data/' directory for chunks.")


if __name__ == "__main__":
    run()
