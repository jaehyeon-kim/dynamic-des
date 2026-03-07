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


def test_sim_parameter_structure():
    """Ensure the SimParameter correctly holds nested services and resources."""
    param = SimParameter(
        param_id="Line_A",
        arrival=DistributionConfig(dist="exponential", rate=0.2),
        service={"step1": DistributionConfig(dist="normal", mean=5, std=1)},
        resources={"operator": ResourceConfig(current_cap=1, max_cap=2)},
    )
    assert param.param_id == "Line_A"
    assert "step1" in param.service
    assert param.resources["operator"].max_cap == 2
