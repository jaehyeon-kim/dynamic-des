from dataclasses import dataclass, field
from typing import Dict, Literal, Optional


@dataclass
class DistributionConfig:
    """
    Configuration for a statistical distribution used in the simulation.

    This config is dynamically updatable. For example, you can change the `rate`
    of an 'exponential' distribution via Kafka, and the `Sampler` will instantly
    use the new rate for the next event.

    Attributes:
        dist (str): The type of distribution (e.g., 'exponential', 'normal', 'uniform').
        rate (Optional[float]): The rate parameter (lambda) for exponential distributions.
        mean (Optional[float]): The mean (mu) for normal distributions.
        std (Optional[float]): The standard deviation (sigma) for normal distributions.
    """

    dist: Literal["exponential", "normal", "lognormal"]
    rate: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None


@dataclass
class CapacityConfig:
    """
    Configuration for the capacity of a simulated resource, container, or store.

    Attributes:
        current_cap (int): The currently active capacity (e.g., number of active workers).
        max_cap (int): The absolute physical maximum capacity. `current_cap` cannot exceed this.
    """

    current_cap: int
    max_cap: int


@dataclass
class SimParameter:
    """
    The master schema representing the state of a specific simulation entity (like a production line).

    This object is registered with the `SimulationRegistry`, which flattens
    the nested dictionaries into dot-notation paths (e.g., 'Line_A.arrival.standard.rate').

    Attributes:
        sim_id (str): The unique prefix for this group of parameters (e.g., 'Line_A').
        arrival (Dict[str, DistributionConfig]): Configurations for arrival generation rates.
        service (Dict[str, DistributionConfig]): Configurations for process task durations.
        resources (Dict[str, CapacityConfig]): Configurations for standard SimPy Resources.
        containers (Dict[str, CapacityConfig]): Configurations for continuous SimPy Containers.
        stores (Dict[str, CapacityConfig]): Configurations for discrete SimPy Stores.
    """

    sim_id: str
    arrival: Dict[str, DistributionConfig] = field(default_factory=dict)
    service: Dict[str, DistributionConfig] = field(default_factory=dict)
    # Standardized categories
    resources: Dict[str, CapacityConfig] = field(default_factory=dict)
    containers: Dict[str, CapacityConfig] = field(default_factory=dict)
    stores: Dict[str, CapacityConfig] = field(default_factory=dict)
