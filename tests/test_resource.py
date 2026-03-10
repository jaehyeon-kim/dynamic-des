from dynamic_des.resources.resource import DynamicResource


def flush_events(env):
    """Process all events scheduled for the current simulation time."""
    while env._queue and env.peek() == env.now:
        env.step()


def test_resource_initialization(env, registry, sample_params):
    """Verify resource initializes with correct registry values."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")

    # Initial capacity from sample_params: current_cap=2, max_cap=5
    assert res.pool.level == 2
    assert res.pool.capacity == 5


def test_basic_request_release(env, registry, sample_params):
    """Verify standard request and release cycle."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")

    def user():
        # Request returns a SimPy Event
        with res.request() as req:
            yield req
            yield env.timeout(10)

    env.process(user())

    # Run slightly to allow dispatcher to grant the token
    env.run(until=1)
    assert res.pool.level == 1  # 1 token taken

    env.run(until=15)
    assert res.pool.level == 2  # token returned


def test_priority_queuing(env, registry, sample_params):
    registry.register_sim_parameter(sample_params)
    registry.update("Line_A.resources.lathe.current_cap", 1)
    flush_events(env)

    res = DynamicResource(env, "Line_A", "lathe")
    log = []

    def user(name, priority, duration):
        with res.request(priority=priority) as req:
            yield req
            log.append(name)
            yield env.timeout(duration)

    # t=0: "First" takes the token, dispatcher then blocks waiting for next token
    env.process(user("First", priority=10, duration=10))
    env.run(until=1)

    # t=1: "LowPrio" enters PriorityStore
    env.process(user("LowPrio", priority=5, duration=1))
    env.run(until=2)

    # t=2: "HighPrio" enters PriorityStore (it is now behind LowPrio in the store)
    env.process(user("HighPrio", priority=1, duration=1))

    # t=10: "First" releases. Dispatcher wakes up, calls queue.get()
    # PriorityStore returns "HighPrio" because 1 < 5.
    env.run()

    assert log == ["First", "HighPrio", "LowPrio"]


def test_dynamic_capacity_increase(env, registry, sample_params):
    """Verify that increasing current_cap in registry grows the pool."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # Capacity starts at 2

    registry.update("Line_A.resources.lathe.current_cap", 4)

    # Advance the clock by a tiny amount to ensure all
    # triggered processes (_watch, _handle, _grow) complete.
    env.run(until=env.now + 0.1)

    assert res._capacity == 4
    assert res.pool.level == 4


def test_dynamic_capacity_decrease_while_busy(env, registry, sample_params):
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # Capacity 2

    def worker():
        with res.request() as req:
            yield req
            yield env.timeout(10)

    env.process(worker())
    env.process(worker())
    env.run(until=1)

    assert res._capacity == 2
    assert res.pool.level == 0

    # Request drop to 1
    registry.update("Line_A.resources.lathe.current_cap", 1)
    env.run(until=env.now + 0.1)

    # Total capacity is now 1, but level is still 0 because
    # the 1 remaining token is still in use by a worker.
    assert res._capacity == 1
    assert res.pool.level == 0

    env.run(until=12)
    assert res.pool.level == 1
