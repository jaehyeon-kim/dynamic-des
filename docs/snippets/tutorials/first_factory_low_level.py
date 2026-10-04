"""Part 1 of the tutorials: the first factory, on the low-level API.

One lathe, parts arriving about every 2 seconds, and 1.5 seconds of machining per
part. Every record is printed to the terminal.
"""

import logging

import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Factory_A",
    arrival={"parts": DistributionConfig(dist="exponential", rate=0.5)},
    service={"machining": DistributionConfig(dist="normal", mean=1.5)},
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=1)},
)


def process_part(env, sampler, lathe, part_id):
    """One part: wait for the lathe, machine it, release it."""
    key = f"task-{part_id}"
    path_id = "Factory_A.service.machining"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})

    with lathe.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        service = env.registry.get_config(path_id)
        yield env.timeout(sampler.sample(service))
        env.publish_event(key, {"part_id": part_id})


def parts_generator(env, sampler, lathe):
    """Starts one process_part for every arrival."""
    arrival = env.registry.get_config("Factory_A.arrival.parts")
    part_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(process_part(env, sampler, lathe, part_id))
        part_id += 1


def run(until=10.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])

    lathe = DynamicResource(env, "Factory_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng())

    env.process(parts_generator(env, sampler, lathe))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    print("Starting simulation...")
    run()
