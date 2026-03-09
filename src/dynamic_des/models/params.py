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

    dist: Literal["exponential", "normal", "lognormal"]
    rate: Optional[float] = None  # For exponential: 1 / mean
    mean: Optional[float] = None  # For normal
    std: Optional[float] = None  # For normal


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
