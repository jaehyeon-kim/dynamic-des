import pytest


def test_registry_registration_paths(registry, sample_params):
    """Verify that SimParameter is flattened into the correct dot-notation paths."""
    registry.register_sim_parameter(sample_params)

    # Line_A.arrival.standard (Exponential) -> .rate
    assert registry.get("Line_A.arrival.standard.rate").value == 1 / 10.0

    # Line_A.arrival.priority (Exponential) -> .rate
    assert registry.get("Line_A.arrival.priority.rate").value == 1 / 50.0

    # Line_A.service.setup (Normal) -> .mean, .std
    assert registry.get("Line_A.service.setup.mean").value == 2.0
    assert registry.get("Line_A.service.setup.std").value == 0.5

    # Line_A.service.milling (Normal) -> .mean, .std
    assert registry.get("Line_A.service.milling.mean").value == 5.0
    assert registry.get("Line_A.service.milling.std").value == 1.2

    # Line_A.resources.lathe -> .current_cap, .max_cap
    assert registry.get("Line_A.resources.lathe.current_cap").value == 2
    assert registry.get("Line_A.resources.lathe.max_cap").value == 5

    # Line_A.resources.operator -> .current_cap, .max_cap
    assert registry.get("Line_A.resources.operator.current_cap").value == 1
    assert registry.get("Line_A.resources.operator.max_cap").value == 3


def test_registry_distribution_update_and_event(registry, sample_params):
    """Verify that updating a service path changes the value and triggers the SimPy event."""
    registry.register_sim_parameter(sample_params)

    target_path = "Line_A.service.setup.mean"
    dyn_val = registry.get(target_path)

    # Capture the current event
    initial_event = dyn_val.change_event
    assert initial_event.triggered is False

    # Perform update
    registry.update(target_path, 3.0)

    # Value must be updated
    assert dyn_val.value == 3.0
    # The event must have been triggered
    assert initial_event.triggered is True
    # A new, untriggered event must be ready for the next update
    assert dyn_val.change_event is not initial_event
    assert dyn_val.change_event.triggered is False


def test_registry_resource_update_and_event(registry, sample_params):
    """Verify that updating a resource path changes the value and triggers the SimPy event."""
    registry.register_sim_parameter(sample_params)

    target_path = "Line_A.resources.lathe.current_cap"
    dyn_val = registry.get(target_path)

    # Capture the current event
    initial_event = dyn_val.change_event
    assert initial_event.triggered is False

    # Perform update
    registry.update(target_path, 5)

    # Value must be updated
    assert dyn_val.value == 5
    # The event must have been triggered
    assert initial_event.triggered is True
    # A new, untriggered event must be ready for the next update
    assert dyn_val.change_event is not initial_event
    assert dyn_val.change_event.triggered is False


def test_registry_missing_path(registry):
    """Ensure that getting or updating a non-existent path behaves correctly."""
    with pytest.raises(KeyError):
        registry.get("Invalid.Path")

    # Updating a missing path should not raise an error but should be handled/logged
    registry.update("Missing.Path", 10)  # Should print warning based on our logic
