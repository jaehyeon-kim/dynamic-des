from dataclasses import dataclass, field
from typing import Dict, Literal, Optional

import numpy as np

# Create a global NumPy random generator for the simulation
rng = np.random.default_rng()


@dataclass
class DistributionConfig:
    """
    Configuration for stochastic timing.
    NOTE: NumPy's exponential uses 'scale' (mean), so we convert 'rate' automatically.
    """

    dist: Literal["exponential", "normal"]
    rate: Optional[float] = None  # For exponential: 1 / mean
    mean: Optional[float] = None  # For normal
    std: Optional[float] = None  # For normal

    def sample(self) -> float:
        """Samples a single value from the distribution using NumPy."""
        if self.dist == "exponential":
            # NumPy uses scale (1/rate).
            # We use max(0.001, ...) to prevent 0 or negative time.
            scale = 1.0 / self.rate if self.rate and self.rate > 0 else 1.0
            return max(0.001, rng.exponential(scale=scale))

        elif self.dist == "normal":
            # loc=mean, scale=std
            val = rng.normal(loc=self.mean or 0, scale=self.std or 0)
            return max(0.001, val)

        return 0.001


@dataclass
class ResourceConfig:
    """Capacity configuration for physical constraints."""

    current_cap: int
    max_cap: int


@dataclass
class SimParameter:
    """
    The unified parameter set for a simulation unit.
    Grouped for easy 'path-based' updates from external backends.
    """

    param_id: str
    arrival: Dict[str, DistributionConfig] = field(default_factory=dict)
    service: Dict[str, DistributionConfig] = field(default_factory=dict)
    resources: Dict[str, ResourceConfig] = field(default_factory=dict)
