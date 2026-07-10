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
    assert res.capacity == 2
    assert res.pool.level == 2
    assert res.pool.capacity == 5


def test_basic_request_release(env, registry, sample_params):
    """Verify standard request and release cycle using context manager."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")

    def user():
        with res.request() as req:
            yield req
            yield env.timeout(10)
        # Release happens automatically after the 'with' block

    env.process(user())

    env.run(until=1)
    assert res.pool.level == 1  # 1 token taken

    env.run(until=15)
    assert res.pool.level == 2  # token returned automatically


def test_request_cancellation(env, registry, sample_params):
    """Verify that leaving the 'with' block before getting a token doesn't break capacity."""
    registry.register_sim_parameter(sample_params)
    registry.update("Line_A.resources.lathe.current_cap", 1)
    flush_events(env)

    res = DynamicResource(env, "Line_A", "lathe")

    def slow_user():
        with res.request() as req:
            yield req
            yield env.timeout(10)

    def impatient_user():
        with res.request() as req:
            # Wait for either the resource OR 5 seconds
            result = yield req | env.timeout(5)
            if req not in result:
                # We timed out! The context manager exits without ever getting the token.
                pass

    # 1. Slow user takes the only slot
    env.process(slow_user())
    env.run(until=1)

    # 2. Impatient user gets in line but leaves after 5 seconds (t=6)
    env.process(impatient_user())
    env.run(until=6)

    # 3. Slow user still has the token. Impatient user left the queue.
    assert res.pool.level == 0

    # 4. Slow user finishes at t=10.
    # The dispatcher will pull the impatient user's ticket, see it is cancelled,
    # and immediately return the token to the pool.
    env.run(until=12)
    assert res.pool.level == 1
    assert res.capacity == 1


def test_priority_queuing(env, registry, sample_params):
    """Verify priority jumping with context manager."""
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

    env.process(user("First", priority=10, duration=10))
    env.run(until=1)

    env.process(user("LowPrio", priority=5, duration=1))
    env.run(until=2)

    env.process(user("HighPrio", priority=1, duration=1))

    env.run()
    assert log == ["First", "HighPrio", "LowPrio"]


def test_dynamic_capacity_increase(env, registry, sample_params):
    """Verify that increasing current_cap in registry grows the pool."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # Capacity starts at 2

    registry.update("Line_A.resources.lathe.current_cap", 4)
    env.run(until=env.now + 0.1)

    assert res.capacity == 4
    assert res.pool.level == 4


def test_dynamic_capacity_decrease_while_busy(env, registry, sample_params):
    """Verify capacity reduction waits for tasks to finish."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # Capacity 2

    def worker():
        with res.request() as req:
            yield req
            yield env.timeout(10)

    env.process(worker())
    env.process(worker())
    env.run(until=1)

    assert res.capacity == 2
    assert res.pool.level == 0

    # Request drop to 1
    registry.update("Line_A.resources.lathe.current_cap", 1)
    env.run(until=env.now + 0.1)

    # Total capacity target is now 1, but level is still 0 because
    # the 1 remaining token is still in use by a worker.
    assert res.capacity == 1
    assert res.pool.level == 0

    env.run(until=12)
    assert res.pool.level == 1
