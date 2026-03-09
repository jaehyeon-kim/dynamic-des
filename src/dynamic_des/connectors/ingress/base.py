import queue


class BaseIngress:
    """Base class for data sources entering the simulation."""

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Listen to external source and push (path, value) tuples to the ingress_queue.
        To be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement the run method.")
