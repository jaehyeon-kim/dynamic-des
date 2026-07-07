from unittest.mock import MagicMock, patch

import pytest

from dynamic_des import SimulationContext


def test_builder_state_management():
    """Validates that the fluent API correctly aggregates all configuration states."""
    app = (
        SimulationContext("TestSim", factor=0.0)
        .add_resource("lathe", current_cap=1, max_cap=5)
        .add_container("wear_structural", current_cap=0.0, max_cap=100.0)  # NEW
        .add_variable("velocity", {"type": "abrupt", "value": 5.0})  # NEW
        .add_service("milling", dist="normal", mean=5.0, std=1.0)
        .add_arrival("standard", dist="exponential", rate=2.0)
        .with_batching(batch_size=100, flush_interval=0.5)
    )

    # Check discrete resources
    assert "lathe" in app._resources_config
    assert app._resources_config["lathe"].max_cap == 5

    # Check continuous containers (NEW)
    assert "wear_structural" in app._containers_config
    assert app._containers_config["wear_structural"].max_cap == 100.0

    # Check dynamic variables (NEW)
    assert "velocity" in app._variables_config
    assert app._variables_config["velocity"]["value"] == 5.0

    # Check statistical distributions
    assert "milling" in app._services_config
    assert app._services_config["milling"].mean == 5.0
    assert "standard" in app._arrivals_config
    assert app._arrivals_config["standard"].rate == 2.0

    # Check batching state
    assert app._batch_size == 100
    assert app._flush_interval == 0.5


def test_runtime_errors_before_build():
    """Ensures runtime helpers fail safely if called before .run()."""
    app = SimulationContext("TestSim")

    with pytest.raises(RuntimeError, match="not instantiated until context is run"):
        app.get_resource("lathe")

    with pytest.raises(
        RuntimeError, match="Cannot spawn processes before context is run"
    ):
        app.spawn(MagicMock())

    with pytest.raises(
        RuntimeError, match="Cannot publish telemetry before context is run"
    ):
        app.publish("utilization", 95.0)


def test_env_property_exposes_environment():
    """Ensures the public env property guards the Builder Phase and exposes the environment after .run()."""
    app = SimulationContext("TestSim", factor=0.0)

    with pytest.raises(RuntimeError, match="not attached until context is run"):
        _ = app.env

    # Because factor=0.0, this executes instantly in wall-clock time.
    app.run(until=0.1)

    assert app.env is app._env


def test_decorator_registration():
    """Validates that loops are properly staged for execution."""
    app = SimulationContext("TestSim")

    @app.arrival_loop("standard")
    def dummy_arrival(context):
        pass

    @app.telemetry_loop(interval=1.0)
    def dummy_telemetry(context):
        pass

    # Should register two startup loops internally
    assert len(app._startup_loops) == 2
    assert app._startup_loops[0][1] == "standard"  # Arrival loop includes metadata
    assert app._startup_loops[1][1] is None  # Telemetry loop has no metadata


@patch("dynamic_des.core.context.DynamicRealtimeEnvironment")
def test_simulation_execution_wiring(MockEnv):
    """
    Validates the .run() orchestration logic without running a real SimPy loop.
    Ensures connectors and registry are wired correctly.
    """
    mock_env_instance = MockEnv.return_value
    mock_ingress = MagicMock()
    mock_egress = MagicMock()

    app = (
        SimulationContext("TestSim", factor=0.0)
        .add_ingress(mock_ingress)
        .add_egress(mock_egress)
        .add_resource("lathe", 1, 1)
        .add_container("wear", 0.0, 100.0)
    )

    app.run(until=10)

    # Validate that Engine was created and run
    MockEnv.assert_called_once_with(factor=0.0)
    mock_env_instance.run.assert_called_once_with(until=10)
    mock_env_instance.teardown.assert_called_once()

    # Validate that connectors were passed to the engine
    mock_env_instance.setup_ingress.assert_called_once_with([mock_ingress])
    mock_env_instance.setup_egress.assert_called_once()


@patch("dynamic_des.core.context.DynamicRealtimeEnvironment")
def test_logical_start_time_forwarding(MockEnv):
    """Validates that a logical start time is forwarded to the environment."""
    from datetime import datetime

    start = datetime(2024, 1, 1, 12, 0, 0)
    app = SimulationContext("TestSim", factor=0.0, logical_start_time=start)
    app.run(until=1)

    MockEnv.assert_called_once_with(factor=0.0, logical_start_time=start)


def test_lightweight_integration():
    """
    A full fast-forward integration test to ensure decorators yield correctly,
    the sampler is exposed, and variables/containers can be fetched.
    """
    app = (
        SimulationContext("IntegrationSim", factor=0.0)
        .add_resource("lathe", 1, 5)
        .add_variable("base_temp", 1200.0)
        .add_service("milling", "exponential", rate=10.0)
        .add_arrival("standard", "exponential", rate=5.0)
    )

    execution_state = {
        "tasks_completed": 0,
        "telemetry_run": False,
        "sampler_accessed": False,
    }

    @app.task(service_id="milling", resource_id="lathe")
    def process_part(task_id):
        execution_state["tasks_completed"] += 1
        return {"status": "ok"}

    @app.arrival_loop("standard")
    def arrival_generator(context):
        # Verify the sampler is exposed and accessible to raw generators
        noise = context.sampler.rng.normal(0, 1)
        if noise is not None:
            execution_state["sampler_accessed"] = True

        yield context.wait_for_arrival("standard")
        context.spawn(process_part(1))

    @app.telemetry_loop(interval=1.0)
    def telemetry_generator(context):
        # Verify variables made it into the switchboard
        temp = context._env.registry.get("IntegrationSim.variables.base_temp").value
        assert temp == 1200.0
        execution_state["telemetry_run"] = True

    # Run for a short duration.
    # Because factor=0.0, this executes instantly in wall-clock time.
    app.run(until=2)

    # If the simulation ran correctly, all loops should have fired successfully
    assert execution_state["tasks_completed"] > 0
    assert execution_state["telemetry_run"] is True
    assert execution_state["sampler_accessed"] is True
