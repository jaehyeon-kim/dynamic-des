import logging
import random
import time
from datetime import datetime

from dynamic_des import RedisEgress, RedisIngress, SimulationContext

logger = logging.getLogger(__name__)

REDIS_URL = "redis://localhost:6379/0"

app = (
    SimulationContext(sim_id="Factory", factor=1.0)
    .add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
    .add_egress(RedisEgress(REDIS_URL, stream_name="events"))
    .add_arrival("part_arrival", dist="exponential", rate=2.0)
)


@app.arrival_loop("part_arrival")
def part_generator(context):
    part_id = 1
    while True:
        yield context.wait_for_arrival("part_arrival")

        part_event = {
            "__stream__": "part_events",
            "part_id": part_id,
            "type": random.choice(["A", "B", "C"]),
            "timestamp": datetime.utcnow().isoformat(),
            "status": "arrived",
        }
        
        context.publish("factory_event", part_event)
        part_id += 1


def run():
    logger.info("Starting Declarative Redis Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running via redis-cli: PUBLISH simulation_params '{\"param_path\": \"Factory.arrival.part_arrival.rate\", \"param_value\": 10.0}'"
    )
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
