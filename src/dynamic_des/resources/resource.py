from __future__ import annotations

from simpy import Container, Environment, Event, PriorityItem, PriorityStore

from dynamic_des.resources.base import BaseDynamicResource


class DynamicResourceRequest(Event):
    """A context manager for DynamicResource requests."""

    def __init__(self, resource: DynamicResource, priority: int):
        super().__init__(resource.env)
        self.resource = resource
        self.cancelled = False  # Track if user leaves the queue before getting a token

        # Put this request into the priority queue
        self.resource.queue.put(PriorityItem(priority, self))

        # Wake up the dispatcher
        if not self.resource._request_event.triggered:
            self.resource._request_event.succeed()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.triggered:
            # The context manager exited before the dispatcher granted the token
            # (e.g., due to a timeout). Mark it so the dispatcher ignores it.
            self.cancelled = True
        else:
            # The task actually held the token, so we must release it back to the pool.
            self.resource.release()


class DynamicResource(BaseDynamicResource):
    """
    A hybrid SimPy resource managing priority-based queuing and dynamic capacity.
    """

    def __init__(self, env: Environment, sim_id: str, resource_id: str):
        super().__init__(env, sim_id, resource_id, "resources")

        max_cap = int(self._max_cap_val.value)
        # Prevent initialization out-of-bounds
        self._capacity = max(0, min(int(self._current_cap_val.value), max_cap))

        self.queue = PriorityStore(env)
        self.pool = Container(env, init=self._capacity, capacity=max_cap)

        self._request_event = self.env.event()
        self.env.process(self._dispatcher())

    @property
    def in_use(self) -> int:
        """Currently occupied slots (Total Capacity - Idle Tokens)."""
        return self._capacity - self.pool.level

    def request(self, priority: int = 1):
        """Returns a context-manager enabled request."""
        return DynamicResourceRequest(self, priority)

    def release(self):
        """Return a token to the pool."""
        return self.pool.put(1)

    def _dispatcher(self):
        """Bridges PriorityStore and Container without pre-fetching tokens."""
        while True:
            # If no one is waiting, sleep until a request is made
            if not self.queue.items:
                yield self._request_event
                self._request_event = self.env.event()

            # Wait for a physical token to be available
            yield self.pool.get(1)

            # Pull the highest-priority request currently in the queue
            ticket = yield self.queue.get()

            # If the user cancelled their request (e.g., timed out while waiting),
            # return the token to the pool and move to the next person.
            if getattr(ticket.item, "cancelled", False):
                self.pool.put(1)
                continue

            # Signal the user process
            if not ticket.item.triggered:
                ticket.item.succeed()

    def _handle_capacity_change(self, new_target: int):
        # Safely bind the new target to physical limits [0, max_cap]
        new_target = max(0, min(new_target, self.pool.capacity))

        diff = new_target - self._capacity
        if diff > 0:
            self._capacity += diff
            self.env.process(self._grow_pool(diff))
        elif diff < 0:
            amount = abs(diff)
            self._capacity -= amount
            self.env.process(self._shrink_pool(amount))

    def _grow_pool(self, amount: int):
        yield self.pool.put(amount)

    def _shrink_pool(self, amount: int):
        yield self.pool.get(amount)
