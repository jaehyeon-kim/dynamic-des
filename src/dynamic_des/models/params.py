from dataclasses import dataclass, field
from typing import Dict, Literal, Optional


@dataclass
class DistributionConfig:
    """
    Configuration for stochastic timing.
    """

    dist: Literal["exponential", "normal", "lognormal"]
    rate: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None


@dataclass
class CapacityConfig:
    """Standardized config for any capacity-constrained object."""

    current_cap: int
    max_cap: int


@dataclass
class SimParameter:
    """
    The unified parameter set for a simulation unit.
    Grouped for easy 'path-based' updates from external backends.
    """

    sim_id: str
    arrival: Dict[str, DistributionConfig] = field(default_factory=dict)
    service: Dict[str, DistributionConfig] = field(default_factory=dict)
    # Standardized categories
    resources: Dict[str, CapacityConfig] = field(default_factory=dict)
    containers: Dict[str, CapacityConfig] = field(default_factory=dict)
    stores: Dict[str, CapacityConfig] = field(default_factory=dict)
