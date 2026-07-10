import logging
import random
from datetime import datetime

import numpy as np

from dynamic_des import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    RedisEgress,
    RedisIngress,
    Sampler,
    SimParameter,
)

logger = logging.getLogger("redis_example")

REDIS_URL = "redis://localhost:6379/0"


def run():
    params = SimParameter(
        sim_id="Factory",
        arrival={"part_arrival": DistributionConfig(dist="exponential", rate=2.0)},
    )

    # Attach egress instance
    egress = RedisEgress(REDIS_URL, stream_name="events")

    # Attach ingress for dynamic parameter updates
    ingress = RedisIngress(REDIS_URL, channel_name="simulation_params")

    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(params)
    env.setup_egress([egress])
    env.setup_ingress([ingress])

    sampler = Sampler(rng=np.random.default_rng(42))

    def part_process(env: DynamicRealtimeEnvironment):
        arrival_cfg = env.registry.get_config("Factory.arrival.part_arrival")
        part_id = 1

        while True:
            yield env.timeout(sampler.sample(arrival_cfg))

            part_event = {
                "__stream__": "part_events",
                "part_id": part_id,
                "type": random.choice(["A", "B", "C"]),
                "timestamp": datetime.utcnow().isoformat(),
                "status": "arrived",
            }
            env.publish_event(f"part-{part_id}", part_event)

            part_id += 1

    env.process(part_process(env))

    logger.info("Starting Imperative Redis Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running via redis-cli: PUBLISH simulation_params '{\"param_path\": \"Factory.arrival.part_arrival.rate\", \"param_value\": 10.0}'"
    )

    try:
        env.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
