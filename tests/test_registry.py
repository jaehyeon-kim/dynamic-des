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


def test_registry_get_config(registry, sample_params):
    """Verify that config objects can be retrieved by path."""
    registry.register_sim_parameter(sample_params)

    # Retrieve DistributionConfig
    setup_cfg = registry.get_config("Line_A.service.setup")
    assert setup_cfg.mean == 2.0
    assert setup_cfg.dist == "normal"

    # Retrieve CapacityConfig
    lathe_cfg = registry.get_config("Line_A.resources.lathe")
    assert lathe_cfg.current_cap == 2
    assert lathe_cfg.max_cap == 5


def test_registry_config_sync(registry, sample_params):
    """Verify that updating a path also updates the parent config object attributes."""
    registry.register_sim_parameter(sample_params)

    # Get reference to the config object
    milling_cfg = registry.get_config("Line_A.service.milling")
    lathe_cfg = registry.get_config("Line_A.resources.lathe")

    # Update through registry
    registry.update("Line_A.service.milling.mean", 99.0)
    registry.update("Line_A.resources.lathe.current_cap", 10)

    # The config objects themselves should be updated via reference
    assert milling_cfg.mean == 99.0
    assert lathe_cfg.current_cap == 10


def test_registry_distribution_update_and_event(registry, sample_params):
    """Verify that updating a service path changes the value and signals the store."""
    registry.register_sim_parameter(sample_params)

    target_path = "Line_A.service.setup.mean"
    dyn_val = registry.get(target_path)

    # Initially, the signal store should be empty
    assert len(dyn_val._signal.items) == 0

    # Update the value
    registry.update(target_path, 10.0)

    # 1. Value must be updated
    assert dyn_val.value == 10.0
    # 2. The signal store should now contain a signal
    assert len(dyn_val._signal.items) == 1


def test_registry_resource_update_and_event(registry, sample_params):
    """Verify that updating a resource path changes the value and signals the store."""
    registry.register_sim_parameter(sample_params)

    target_path = "Line_A.resources.lathe.current_cap"
    dyn_val = registry.get(target_path)

    # Initially, the signal store should be empty
    assert len(dyn_val._signal.items) == 0

    # Perform surgical update
    registry.update(target_path, 5)

    # 1. Value must be updated
    assert dyn_val.value == 5
    # 2. The signal store should now contain a signal
    assert len(dyn_val._signal.items) == 1


def test_registry_missing_path(registry):
    """Ensure that getting or updating a non-existent path behaves correctly."""
    with pytest.raises(KeyError):
        registry.get("Invalid.Path")

    with pytest.raises(KeyError):
        registry.get_config("Invalid.Config.Path")

    # Updating a missing path should not raise an error but should be handled/logged
    registry.update("Missing.Path", 10)  # Should print warning based on our logic
