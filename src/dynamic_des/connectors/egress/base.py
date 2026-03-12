import queue


class BaseEgress:
    """
    Base class for all egress providers in the simulation.

    Egress providers act as asynchronous bridges that consume processed
    simulation data (telemetry and events) from a thread-safe internal queue
    and transmit it to external destinations such as Kafka, databases,
    or the console.
    """

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        Listens to the internal queue and pushes data to an external sink.

        This method should contain an asynchronous loop that polls the
        provided queue and handles the networking/I/O logic specific
        to the destination system.

        Args:
            egress_queue: A thread-safe queue containing batches of
                dictionaries to be exported.

        Raises:
            NotImplementedError: If the subclass does not override this method.
        """
        raise NotImplementedError("Subclasses must implement the run method.")
