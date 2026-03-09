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

# 1. Define initial system state
line_a_params = SimParameter(
    sim_id="Line_A",
    arrival={
        "standard": DistributionConfig(dist="exponential", rate=0.2)
    },  # 1 every 5s
    service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},
)

# 2. Setup Environment with Local Connectors
# Capacity will increase from 1 to 3 at 10 seconds simulation time
ingress = LocalIngress(schedule=[(10.0, "Line_A.resources.lathe.current_cap", 3)])
egress = ConsoleEgress()

env = DynamicRealtimeEnvironment(factor=1.0)  # 1 sim unit = 1 wall-clock second
env.registry.register_sim_parameter(line_a_params)
env.setup_ingress([ingress])
env.setup_egress([egress])

# 3. Initialize Resources and Utilities
res = DynamicResource(env, "Line_A", "lathe")
sampler = Sampler(rng=np.random.default_rng(42))


# 4. Define Simulation Logic
def arrival_process(env, res):
    arrival_rate = env.registry.get("Line_A.arrival.standard.rate")
    service_cfg = line_a_params.service["milling"]

    while True:
        # Wait for next entity (Dynamic arrival rate)
        yield env.timeout(
            sampler.sample(
                DistributionConfig(dist="exponential", rate=arrival_rate.value)
            )
        )
        env.process(work_task(env, res, service_cfg))


def work_task(env, res, cfg):
    print(f"[{env.now:.2f}] Task waiting for Lathe. (Current Cap: {res._capacity})")
    yield res.request()

    yield env.timeout(sampler.sample(cfg))

    res.release()
    print(f"[{env.now:.2f}] Task finished.")


def telemetry_monitor(env, res):
    """Publish rich data for Dashboards."""
    while True:
        # Stream the full state of the resource
        env.publish_telemetry("Line_A.lathe.capacity", res._capacity)
        env.publish_telemetry("Line_A.lathe.in_use", res.in_use)
        env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))

        util = (res.in_use / res._capacity) * 100 if res._capacity > 0 else 0
        env.publish_telemetry("Line_A.lathe.utilization", util)

        yield env.timeout(1.0)


# 5. Run
env.process(arrival_process(env, res))
env.process(telemetry_monitor(env, res))

print("Simulation started. Watch capacity change at t=10.0s...")
env.run(until=30)
