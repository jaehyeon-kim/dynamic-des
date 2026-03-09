from simpy import Container, Environment, PriorityItem, PriorityStore

from dynamic_des.resources.base import BaseDynamicResource


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
        """Returns an event that succeeds when a token is granted."""
        granted = self.env.event()
        self.queue.put(PriorityItem(priority, granted))

        # Trigger the dispatcher to wake up
        if not self._request_event.triggered:
            self._request_event.succeed()

        return granted

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
