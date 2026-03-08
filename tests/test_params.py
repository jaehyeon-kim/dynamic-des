import numpy as np

from dynamic_des.models.params import DistributionConfig, ResourceConfig, SimParameter


def test_exponential_sampling():
    """Verify that exponential sampling uses the correct rate-to-scale conversion."""
    # Mean of 10.0 -> Rate of 0.1
    config = DistributionConfig(dist="exponential", rate=0.1)

    # Generate a large sample size for statistical verification
    samples = [config.sample() for _ in range(2000)]

    # Check bounds
    assert min(samples) >= 0.001
    # Check mean (should be approximately 1/rate = 10)
    assert 9.5 < np.mean(samples) < 10.5


def test_normal_sampling_safety():
    """Verify that Normal distribution never returns a non-positive value."""
    # Mean of 1.0 with high Std of 10.0 will frequently produce negatives mathematically
    config = DistributionConfig(dist="normal", mean=1.0, std=10.0)

    samples = [config.sample() for _ in range(1000)]

    # Simulation must never receive 0 or negative time
    assert min(samples) >= 0.001


def test_sim_parameter_structure(sample_params):
    """Ensure the SimParameter correctly holds nested services and resources."""
    assert sample_params.param_id == "Line_A"
    assert "setup" in sample_params.service
    assert sample_params.resources["operator"].max_cap == 3
