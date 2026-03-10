from __future__ import annotations

from simpy import Container, Environment, Event, PriorityItem, PriorityStore

from dynamic_des.resources.base import BaseDynamicResource


class DynamicResourceRequest(Event):
    """A context manager for DynamicResource requests."""

    def __init__(self, resource: DynamicResource, priority: int):
        super().__init__(resource.env)
        self.resource = resource

        # Put this request into the priority queue
        self.resource.queue.put(PriorityItem(priority, self))

        # Wake up the dispatcher
        if not self.resource._request_event.triggered:
            self.resource._request_event.succeed()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.resource.release()


class DynamicResource(BaseDynamicResource):
    def __init__(self, env: Environment, sim_id: str, resource_id: str):
        super().__init__(env, sim_id, resource_id, "resources")

        # Track total capacity (tokens in pool + tokens in use)
        self._capacity = int(self._current_cap_val.value)

        self.queue = PriorityStore(env)
        self.pool = Container(
            env, init=self._capacity, capacity=int(self._max_cap_val.value)
        )
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

            # Signal the user process
            if not ticket.item.triggered:
                ticket.item.succeed()

    def _handle_capacity_change(self, new_target: int):
        diff = new_target - self._capacity
        if diff > 0:
            amount = min(diff, self.pool.capacity - self._capacity)
            if amount > 0:
                self._capacity += amount
                self.env.process(self._grow_pool(amount))
        elif diff < 0:
            amount = abs(diff)
            self._capacity -= amount
            self.env.process(self._shrink_pool(amount))

    def _grow_pool(self, amount: int):
        yield self.pool.put(amount)

    def _shrink_pool(self, amount: int):
        yield self.pool.get(amount)
