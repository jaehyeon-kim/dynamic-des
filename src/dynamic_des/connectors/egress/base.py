import queue


class BaseEgress:
    """Base class for data leaving the simulation."""

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        Listen to the internal output_queue and push data to the external sink.
        To be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement the run method.")
