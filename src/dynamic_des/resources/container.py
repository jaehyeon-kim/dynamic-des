from simpy import Container, Environment, Event
from simpy.resources.container import ContainerGet

from dynamic_des.resources.base import BaseDynamicResource


class DynamicContainer(BaseDynamicResource):
    """
    A wrapper for SimPy Container with dynamic capacity updates.

    Useful for modeling continuous bulk materials (fluids, gases, battery charge).
    Unlike a standard `simpy.Container`, the capacity of this container listens
    to a `SimulationRegistry` path and can be mutated at runtime.

    **Capacity Shrinkage Behavior:**
    If the capacity is dynamically shrunk below the current `level` of the container,
    the material is NOT destroyed. The container will temporarily exist in an
    "overflow" state. All pending and future `put` requests will be strictly
    blocked until downstream processes `get` enough material to drain the `level`
    back below the new, smaller capacity.

    Attributes:
        env (Environment): The active SimPy simulation environment.
        sim_id (str): The parent simulation ID prefix.
        obj_id (str): The specific container name/identifier.
    """

    def __init__(
        self, env: Environment, sim_id: str, container_id: str, init: float = 0
    ):
        """
        Initializes the DynamicContainer and binds its capacity to the registry.

        Args:
            env (Environment): The SimPy environment (must include Registry interface).
            sim_id (str): The parent simulation ID.
            container_id (str): The unique name of this container.
            init (float, optional): The initial amount of material in the container
                at simulation start. Defaults to 0.
        """
        super().__init__(env, sim_id, container_id, "containers")

        max_cap = float(self._max_cap_val.value)
        # Prevent initialization out-of-bounds
        initial_capacity = max(0.0, min(float(self._current_cap_val.value), max_cap))

        self._container = Container(env, init=init, capacity=initial_capacity)

    @property
    def capacity(self) -> float:
        """
        float: The current active maximum capacity of the container.
        This value reflects the live state from the Registry.
        """
        return self._container.capacity

    @property
    def level(self) -> float:
        """float: The current amount of bulk material stored inside the container."""
        return self._container.level

    def put(self, amount: float) -> Event:
        """
        Request to put a specific amount of material into the container.

        Args:
            amount (float): The amount of material to add.

        Returns:
            simpy.events.Event: A SimPy event that triggers when capacity is available.
        """
        return self._container.put(amount)

    def get(self, amount: float) -> ContainerGet:
        """
        Request to get a specific amount of material from the container.

        Args:
            amount (float): The amount of material to withdraw.

        Returns:
            simpy.resources.container.ContainerGet: A SimPy event that triggers
                when enough material is available.
        """
        return self._container.get(amount)

    def _handle_capacity_change(self, new_target: float):
        """
        Internal callback triggered when the Registry capacity value changes.
        Updates the physical SimPy container and processes pending events.

        Args:
            new_target (float): The new capacity limit dictated by the control plane.
        """
        # Safely bind the new target to absolute physical limits [0, max_cap]
        max_cap = float(self._max_cap_val.value)
        new_target = max(0.0, min(float(new_target), max_cap))

        self._container._capacity = new_target

        # If capacity grew, pending put requests might now have enough room to succeed.
        # We manually trigger SimPy's internal put processor to wake them up.
        if self._container.put_queue:
            self._container._trigger_put(None)
