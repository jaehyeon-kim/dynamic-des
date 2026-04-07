from typing import Any

from simpy import Environment, Event, PriorityStore, Store
from simpy.resources.store import StoreGet

from dynamic_des.resources.base import BaseDynamicResource


class DynamicStore(BaseDynamicResource):
    """
    A wrapper for SimPy Store and PriorityStore with dynamic capacity updates.

    Useful for modeling buffers, queues, or staging areas that hold discrete,
    heterogeneous items. By setting `priority=True`, the store will automatically
    sort incoming items (which must be sortable or wrapped in `simpy.PriorityItem`)
    so high-priority items are retrieved first.

    **Capacity Shrinkage Behavior:**
    If the capacity is dynamically shrunk below the current number of `items`
    in the store, the existing items are NOT destroyed. The store will temporarily
    exist in an "over-capacity" state. All pending and future `put` requests
    will be blocked until downstream processes `get` enough items to bring the
    total item count below the new capacity.

    Attributes:
        env (Environment): The active SimPy simulation environment.
        sim_id (str): The parent simulation ID prefix.
        obj_id (str): The specific store name/identifier.
    """

    def __init__(
        self, env: Environment, sim_id: str, store_id: str, priority: bool = False
    ):
        """
        Initializes the DynamicStore and binds its capacity to the registry.

        Args:
            env (Environment): The SimPy environment (must include Registry interface).
            sim_id (str): The parent simulation ID.
            store_id (str): The unique name of this store.
            priority (bool): If True, uses `simpy.PriorityStore` instead of
                the standard FIFO `simpy.Store`. Defaults to False.
        """
        super().__init__(env, sim_id, store_id, "stores")

        max_cap = int(self._max_cap_val.value)
        # Prevent initialization out-of-bounds
        initial_capacity = max(0, min(int(self._current_cap_val.value), max_cap))

        # Dynamically select the underlying SimPy storage engine
        if priority:
            self._store = PriorityStore(env, capacity=initial_capacity)
        else:
            self._store = Store(env, capacity=initial_capacity)

    @property
    def capacity(self) -> int:
        """
        int: The current active maximum slot capacity of the store.
        This value reflects the live state from the Registry.
        """
        return self._store.capacity

    @property
    def items(self) -> list:
        """
        list: The actual list of distinct item objects currently residing
        in the store.
        """
        return self._store.items

    def put(self, item: Any) -> Event:
        """
        Request to put a specific distinct item into the store.

        Args:
            item (Any): The Python object to place into the store.

        Returns:
            simpy.events.Event: A SimPy event that triggers when a slot is available.
        """
        return self._store.put(item)

    def get(self) -> StoreGet:
        """
        Request to get the next available item from the store.

        Returns:
            simpy.resources.store.StoreGet: A SimPy event that yields the
                requested item object when available.
        """
        return self._store.get()

    def _handle_capacity_change(self, new_target: int):
        """
        Internal callback triggered when the Registry capacity value changes.
        Updates the physical SimPy store and processes pending events.

        Args:
            new_target (int): The new slot capacity limit dictated by the control plane.
        """
        # Safely bind the new target to absolute physical limits [0, max_cap]
        max_cap = int(self._max_cap_val.value)
        new_target = max(0, min(int(new_target), max_cap))

        self._store._capacity = new_target

        # If capacity grew, pending put requests might now have open slots to succeed.
        # This exact same trigger works for both Store and PriorityStore.
        if self._store.put_queue:
            self._store._trigger_put(None)
