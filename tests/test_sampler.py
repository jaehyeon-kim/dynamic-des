import numpy as np
import pytest

from dynamic_des.core.sampler import Sampler
from dynamic_des.models.params import DistributionConfig


@pytest.fixture
def rng():
    """Provides a seeded NumPy generator for reproducible stochastic tests."""
    return np.random.default_rng(42)


@pytest.fixture
def sampler(rng):
    """Provides a Sampler initialized with an RNG."""
    return Sampler(rng=rng)


@pytest.fixture
def deterministic_sampler():
    """Provides a Sampler with no RNG for deterministic tests."""
    return Sampler(rng=None)


def test_sampler_deterministic_fallback(deterministic_sampler):
    """Verify that without an RNG, the sampler returns the mean (clipped)."""
    # Exponential: mean = 1/0.1 = 10.0
    exp_cfg = DistributionConfig(dist="exponential", rate=0.1)
    assert deterministic_sampler.sample(exp_cfg) == 10.0

    # Normal: mean = 5.0
    norm_cfg = DistributionConfig(dist="normal", mean=5.0, std=1.0)
    assert deterministic_sampler.sample(norm_cfg) == 5.0

    # Lognormal: mean = 3.0
    log_cfg = DistributionConfig(dist="lognormal", mean=3.0, std=0.5)
    assert deterministic_sampler.sample(log_cfg) == 3.0


def test_sampler_min_delay_clipping(sampler):
    """Verify that min_delay correctly clips small or negative values."""
    # Create a normal distribution that would produce a negative value
    # (Seed 42 first normal sample is ~0.3, so mean -5.0 will be negative)
    cfg = DistributionConfig(dist="normal", mean=-5.0, std=1.0)

    # Test with default 0.1
    assert sampler.sample(cfg, min_delay=0.1) == 0.1

    # Test with custom 1.0
    assert sampler.sample(cfg, min_delay=1.0) == 1.0


def test_sampler_stochastic_exponential(sampler):
    """Verify exponential sampling (rate to scale conversion)."""
    rate = 0.5  # Mean should be 2.0
    cfg = DistributionConfig(dist="exponential", rate=rate)

    samples = [sampler.sample(cfg) for _ in range(1000)]
    assert 1.9 < np.mean(samples) < 2.1
    assert min(samples) >= 0.1


def test_sampler_stochastic_lognormal(sampler):
    """Verify lognormal sampling returns values near the desired mean."""
    desired_mean = 10.0
    desired_std = 2.0
    cfg = DistributionConfig(dist="lognormal", mean=desired_mean, std=desired_std)

    samples = [sampler.sample(cfg) for _ in range(1000)]
    # Log-normal mean is usually close to desired mean in large samples
    assert 9.5 < np.mean(samples) < 10.5
    assert min(samples) > 0  # Log-normal is strictly positive


def test_sampler_invalid_config(sampler):
    """Verify fallback when config has missing values."""
    # Normal with no mean provided
    cfg = DistributionConfig(dist="normal", mean=None, std=None)
    assert sampler.sample(cfg, min_delay=0.5) == 0.5
