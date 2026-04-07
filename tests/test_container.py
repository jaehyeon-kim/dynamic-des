from dynamic_des.resources.container import DynamicContainer


def flush_events(env):
    """Process all events scheduled for the current simulation time."""
    while env._queue and env.peek() == env.now:
        env.step()


def test_container_initialization(env, registry, sample_params):
    """Verify container initializes with correct registry values and starting level."""
    registry.register_sim_parameter(sample_params)

    cont = DynamicContainer(env, "Line_A", "tank1", init=10.0)

    assert cont.capacity == 50.0
    assert cont.level == 10.0


def test_basic_put_get(env, registry, sample_params):
    """Verify standard put and get cycle."""
    registry.register_sim_parameter(sample_params)
    cont = DynamicContainer(env, "Line_A", "tank1", init=10.0)

    def process_flow():
        yield cont.put(20.0)  # Level becomes 30
        yield env.timeout(5)
        yield cont.get(15.0)  # Level becomes 15

    env.process(process_flow())
    env.run(until=1)
    assert cont.level == 30.0

    env.run(until=10)
    assert cont.level == 15.0


def test_dynamic_capacity_increase(env, registry, sample_params):
    """Verify that increasing capacity unblocks pending put requests."""
    registry.register_sim_parameter(sample_params)
    cont = DynamicContainer(env, "Line_A", "tank1", init=40.0)

    def producer():
        # Tank only has 10 space left. Putting 30 will block.
        yield cont.put(30.0)

    env.process(producer())
    env.run(until=5)

    # The put is blocked. Level is still 40.
    assert cont.level == 40.0

    # Expand capacity to 100 via control plane update.
    registry.update("Line_A.containers.tank1.current_cap", 100.0)
    env.run(until=env.now + 0.1)

    # The pending 30 should instantly flow in.
    assert cont.capacity == 100.0
    assert cont.level == 70.0


def test_capacity_shrinkage_paradox(env, registry, sample_params):
    """Verify that shrinking capacity safely blocks future puts without destroying matter."""
    registry.register_sim_parameter(sample_params)
    cont = DynamicContainer(env, "Line_A", "tank1", init=50.0)

    def process_flow():
        # We try to put 10 more in.
        yield cont.put(10.0)

    # Shrink capacity from 50 down to 30.
    registry.update("Line_A.containers.tank1.current_cap", 30.0)
    flush_events(env)

    # Tank is overflowing mathematically, but material is preserved
    assert cont.capacity == 30.0
    assert cont.level == 50.0

    # Try to put more in. It should block.
    env.process(process_flow())
    env.run(until=5)
    assert cont.level == 50.0  # Put is still blocked

    # Drain the tank below the new capacity
    def consumer():
        yield cont.get(
            30.0
        )  # Drains 50 -> 20. The pending 10 can now fit (20 + 10 <= 30)

    env.process(consumer())
    env.run(until=6)

    # The consumer took 30, and the pending put of 10 was instantly processed
    assert cont.level == 30.0  # 50 - 30 + 10
