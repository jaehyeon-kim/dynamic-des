import numpy as np

from dynamic_des.models.params import SimParameter


def test_sim_parameter_structure(sample_params):
    """Verify that sample_params matches the expected hierarchical structure."""
    assert isinstance(sample_params, SimParameter)
    assert sample_params.sim_id == "Line_A"

    # Check Arrivals
    assert "standard" in sample_params.arrival
    assert sample_params.arrival["standard"].rate == 1 / 10.0

    # Check Services
    assert "milling" in sample_params.service
    assert sample_params.service["milling"].mean == 5.0

    # Check Resources
    assert "lathe" in sample_params.resources
    assert sample_params.resources["lathe"].current_cap == 2


def test_manual_numpy_sampling(sample_params):
    """Verify how a user would manually sample using NumPy and the config data."""
    rng = np.random.default_rng(42)

    # Sample from arrival (exponential)
    arrival_config = sample_params.arrival["standard"]
    # User-side logic: scale = 1/rate
    wait_time = rng.exponential(scale=1.0 / arrival_config.rate)
    assert wait_time > 0

    # Sample from service (normal)
    service_config = sample_params.service["milling"]
    # User-side logic: loc=mean, scale=std
    work_time = rng.normal(loc=service_config.mean, scale=service_config.std)
    # Manual safety clip
    assert max(0.001, work_time) > 0
