import logging

import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    LocalIngress,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
)
logger = logging.getLogger("local_example")

# 1. Define the system schema
# Line_A starts with 1 lathe, but has a physical ceiling of 5.
line_a_params = SimParameter(
    sim_id="Line_A",
    arrival={"standard": DistributionConfig(dist="exponential", rate=1.0)},
    service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},
)

# 2. Setup Environment with Local Connectors
# Schedule capacity updates: jump to 3 at t=10s, then drop to 2 at t=20s
ingress = LocalIngress(
    schedule=[
        (10.0, "Line_A.resources.lathe.current_cap", 3),
        (20.0, "Line_A.resources.lathe.current_cap", 2),
    ]
)
egress = ConsoleEgress()

env = DynamicRealtimeEnvironment(factor=1.0)
env.registry.register_sim_parameter(line_a_params)
env.setup_ingress([ingress])
env.setup_egress([egress])

# 3. Initialize Resources and Sampler
res = DynamicResource(env, "Line_A", "lathe")
sampler = Sampler(rng=np.random.default_rng(42))


# 4. Define Simulation Logic
def arrival_process(env: DynamicRealtimeEnvironment, res: DynamicResource):
    """Generates tasks based on the dynamic arrival rate."""
    arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
    service_path = "Line_A.service.milling"
    task_id = 0

    while True:
        # Reference-based: arrival_cfg updates automatically via Registry
        yield env.timeout(sampler.sample(arrival_cfg))
        env.process(work_task(env, task_id, res, service_path))
        task_id += 1


def work_task(
    env: DynamicRealtimeEnvironment, task_id: int, res: DynamicResource, path_id: str
):
    """Models task lifecycle: queued -> started -> finished."""
    task_key = f"task-{task_id}"
    env.publish_event(task_key, {"path_id": path_id, "status": "queued"})

    with res.request() as req:
        yield req
        # Late Binding: Fetch latest config only when work actually starts
        current_service_cfg = env.registry.get_config(path_id)

        logger.info(f"Task {task_id} started at sim time: {env.now:.2f}s")
        env.publish_event(task_key, {"path_id": path_id, "status": "started"})

        yield env.timeout(sampler.sample(current_service_cfg))
        env.publish_event(task_key, {"path_id": path_id, "status": "finished"})


def telemetry_monitor(env: DynamicRealtimeEnvironment, res: DynamicResource):
    """Streams system health metrics every 2 seconds."""
    while True:
        env.publish_telemetry("Line_A.resources.lathe.capacity", res._capacity)
        env.publish_telemetry("Line_A.resources.lathe.in_use", res.in_use)
        env.publish_telemetry(
            "Line_A.resources.lathe.queue_length", len(res.queue.items)
        )

        util = (res.in_use / res._capacity) * 100 if res._capacity > 0 else 0
        env.publish_telemetry("Line_A.resources.lathe.utilization", util)
        yield env.timeout(2.0)


# 5. Run the Simulation
env.process(arrival_process(env, res))
env.process(telemetry_monitor(env, res))

print("Simulation started. Watch capacity change at t=10.0s and 20.0s...")
try:
    env.run(until=30)
except KeyboardInterrupt:
    logger.info("Simulation interrupted by user.")
finally:
    env.teardown()
