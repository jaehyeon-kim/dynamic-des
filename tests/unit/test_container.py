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


def test_fractional_capacity_change_is_kept(env, registry, sample_params):
    """Verify a fractional current_cap update is not rounded down."""
    registry.register_sim_parameter(sample_params)
    cont = DynamicContainer(env, "Line_A", "tank1", init=10.0)

    registry.update("Line_A.containers.tank1.current_cap", 62.5)
    env.run(until=env.now + 0.1)

    assert cont.capacity == 62.5


def test_max_cap_decrease_lowers_capacity(env, registry, sample_params):
    """Verify lowering max_cap below current_cap shrinks the capacity at once."""
    registry.register_sim_parameter(sample_params)
    cont = DynamicContainer(env, "Line_A", "tank1", init=10.0)

    registry.update("Line_A.containers.tank1.max_cap", 30.0)
    env.run(until=env.now + 0.1)

    assert cont.capacity == 30.0


def test_integer_starting_capacity_keeps_fractional_updates(env, registry):
    """Verify a container registered with whole numbers still takes 62.5."""
    from dynamic_des.models.params import CapacityConfig, SimParameter

    registry.register_sim_parameter(
        SimParameter(
            sim_id="Line_B",
            containers={"tank": CapacityConfig(current_cap=50, max_cap=100)},
        )
    )
    cont = DynamicContainer(env, "Line_B", "tank", init=10)

    registry.update("Line_B.containers.tank.current_cap", 62.5)
    env.run(until=env.now + 0.1)

    assert cont.capacity == 62.5


def test_resource_classes_are_exported_from_the_package():
    """Verify all three dynamic resource classes import from dynamic_des."""
    import dynamic_des
    from dynamic_des.resources.store import DynamicStore

    assert dynamic_des.DynamicContainer is DynamicContainer
    assert dynamic_des.DynamicStore is DynamicStore
    assert {"DynamicResource", "DynamicContainer", "DynamicStore"} <= set(
        dynamic_des.__all__
    )
