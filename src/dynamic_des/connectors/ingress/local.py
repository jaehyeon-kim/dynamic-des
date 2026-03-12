import asyncio
import queue
from typing import Any, List, Tuple

from dynamic_des.connectors.ingress.base import BaseIngress


class LocalIngress(BaseIngress):
    """
    Ingress provider for deterministic, time-scheduled parameter updates.

    This connector is primarily used for testing, benchmarking, or local
    simulation runs where parameter changes need to occur at specific
    wall-clock intervals without requiring an external message broker like
    Kafka. It feeds a pre-defined sequence of updates into the simulation
    registry based on relative delays.

    Attributes:
        schedule (List[Tuple[float, str, Any]]): A chronologically sorted
            list of updates, where each tuple contains (delay_seconds,
            path_id, value).
    """

    def __init__(self, schedule: List[Tuple[float, str, Any]]):
        """
        Initializes the LocalIngress and sorts the provided schedule.

        Args:
            schedule: A list of tuples representing scheduled updates.
                Format: [(delay_from_start, "registry_path", new_value), ...].
                The list is automatically sorted by the delay time.
        """
        # Sort schedule by delay to ensure chronological processing
        self.schedule = sorted(schedule, key=lambda x: x[0])

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Processes the schedule and pushes updates to the queue at the
        correct wall-clock times.

        This method calculates the relative sleep intervals between
        scheduled events to ensure that updates are placed in the
        ingress queue precisely when requested. Because it uses
        `asyncio.sleep`, it does not block the main simulation execution.

        Args:
            ingress_queue: A thread-safe queue used to transmit the
                (path_id, value) updates to the SimulationRegistry.
        """
        last_time = 0.0
        for delay, path_id, value in self.schedule:
            # Wait for the relative time difference
            await asyncio.sleep(delay - last_time)
            ingress_queue.put((path_id, value))
            last_time = delay
