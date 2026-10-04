"""Python for examples/yaml/redis.yaml: the part generator."""

import random
from datetime import datetime


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
