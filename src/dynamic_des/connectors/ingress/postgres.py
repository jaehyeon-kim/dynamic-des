import queue

from dynamic_des.connectors.ingress.base import BaseIngress


class PostgresIngress(BaseIngress):
    """
    Asynchronous ingress provider for polling updates from a PostgreSQL database.

    This connector is intended to act as a bridge for scenarios where simulation
    parameters are managed within a relational database. It would typically
    poll a configuration table for changes or listen to database notifications
    (LISTEN/NOTIFY) to trigger real-time updates in the simulation registry.

    Note:
        This class is currently a placeholder and is planned for a future release.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the PostgresIngress placeholder.

        Raises:
            NotImplementedError: Always raised as the connector is not yet implemented.
        """
        raise NotImplementedError("PostgresIngress is planned for a future release.")

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Placeholder for the database polling execution loop.

        Future implementations will likely utilize an async driver (e.g., asyncpg)
        to fetch rows and convert them into (path, value) tuples for the
        ingress queue.

        Args:
            ingress_queue: A thread-safe queue used to transmit updates
                to the SimulationRegistry.
        """
        pass
