import asyncio
import queue
from typing import Any, List, Tuple

from dynamic_des.connectors.ingress.base import BaseIngress


class LocalIngress(BaseIngress):
    """
    Feeds a pre-defined schedule of updates into the simulation.
    Schedule format: [(delay_seconds, "path_id", value), ...]
    """

    def __init__(self, schedule: List[Tuple[float, str, Any]]):
        # Sort schedule by delay to ensure chronological processing
        self.schedule = sorted(schedule, key=lambda x: x[0])

    async def run(self, ingress_queue: queue.Queue) -> None:
        last_time = 0.0
        for delay, path_id, value in self.schedule:
            # Wait for the relative time difference
            await asyncio.sleep(delay - last_time)
            ingress_queue.put((path_id, value))
            last_time = delay
