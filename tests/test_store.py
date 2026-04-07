import simpy

from dynamic_des.resources.store import DynamicStore


def flush_events(env):
    """Process all events scheduled for the current simulation time."""
    while env._queue and env.peek() == env.now:
        env.step()


def test_store_initialization(env, registry, sample_params):
    """Verify standard store initializes correctly as FIFO."""
    registry.register_sim_parameter(sample_params)
    store = DynamicStore(env, "Line_A", "buffer1")

    assert store.capacity == 5
    assert isinstance(store._store, simpy.Store)
    assert not isinstance(store._store, simpy.PriorityStore)


def test_store_priority_initialization(env, registry, sample_params):
    """Verify priority flag correctly instantiates a PriorityStore."""
    registry.register_sim_parameter(sample_params)
    store = DynamicStore(env, "Line_A", "buffer1", priority=True)

    assert store.capacity == 5
    assert isinstance(store._store, simpy.PriorityStore)


def test_priority_sorting(env, registry, sample_params):
    """Verify that priority sorting works identically to native SimPy."""
    registry.register_sim_parameter(sample_params)
    store = DynamicStore(env, "Line_A", "buffer1", priority=True)
    out_log = []

    def process_flow():
        # Lower number = higher priority
        yield store.put(simpy.PriorityItem(priority=5, item="Low"))
        yield store.put(simpy.PriorityItem(priority=1, item="High"))

        first = yield store.get()
        out_log.append(first.item)

        second = yield store.get()
        out_log.append(second.item)

    env.process(process_flow())
    env.run(until=1)

    assert out_log == ["High", "Low"]


def test_dynamic_capacity_increase(env, registry, sample_params):
    """Verify increasing slots unblocks pending items."""
    # Force initial capacity to be small for this test
    sample_params.stores["buffer1"].current_cap = 2
    registry.register_sim_parameter(sample_params)

    store = DynamicStore(env, "Line_A", "buffer1")

    def producer():
        yield store.put("item_1")
        yield store.put("item_2")
        yield store.put("item_3")  # Blocks here

    env.process(producer())
    env.run(until=5)

    # 3rd item is blocked
    assert len(store.items) == 2

    # Expand capacity via update
    registry.update("Line_A.stores.buffer1.current_cap", 3)
    env.run(until=env.now + 0.1)

    # 3rd item unblocks and flows in
    assert store.capacity == 3
    assert len(store.items) == 3


def test_capacity_shrinkage_paradox(env, registry, sample_params):
    """Verify shrinking capacity handles over-filled buffer cleanly."""
    sample_params.stores["buffer1"].current_cap = 3
    registry.register_sim_parameter(sample_params)

    store = DynamicStore(env, "Line_A", "buffer1")

    # Fill to 3
    store.put("item_1")
    store.put("item_2")
    store.put("item_3")
    env.run(until=1)

    # Shrink to 1 via update
    registry.update("Line_A.stores.buffer1.current_cap", 1)
    flush_events(env)

    assert store.capacity == 1
    assert len(store.items) == 3  # Buffer is over capacity

    # Add a pending put
    store.put("item_4")
    env.run(until=5)
    assert len(store.items) == 3  # "item_4" is strictly blocked

    # Drain 3 items, making space for "item_4" since new capacity is 1
    def consumer():
        yield store.get()
        yield store.get()
        yield store.get()

    env.process(consumer())
    env.run(until=10)

    # The 3 items left, the pending 4th item finally entered (using the 1 slot)
    assert len(store.items) == 1
