import queue


class BaseIngress:
    """
    Base class for all ingress providers in the simulation.

    Ingress providers act as asynchronous listeners that bridge external
    data sources (such as Kafka topics, Redis channels, or local schedules)
    with the simulation's internal state. They are responsible for fetching
    updates and placing them into a thread-safe queue for the registry to process.
    """

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Listens to an external source and pushes updates to the ingress queue.

        This method should be implemented as an asynchronous loop that waits
        for external signals or data and converts them into (path, value)
        tuples suitable for the SimulationRegistry.

        Args:
            ingress_queue: A thread-safe queue used to transmit updates
                to the main simulation environment.

        Raises:
            NotImplementedError: If the subclass does not override this method.
        """
        raise NotImplementedError("Subclasses must implement the run method.")
