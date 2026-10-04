"""Pydantic models for a blueprint file.

Every model forbids unknown keys, so a misspelt key is reported rather than ignored.
Distributions and capacities reuse `DistributionConfig` and `CapacityConfig`, the
dataclasses the builder already registers.
"""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from dynamic_des.models.params import CapacityConfig, DistributionConfig
from dynamic_des.utils import time_to_seconds

# What a `telemetry` entry can publish about a resource without any Python.
RESOURCE_STATS = ("capacity", "in_use", "queue_length", "utilization")


def _seconds(value: Union[float, str]) -> float:
    """Accepts a number of seconds or a duration such as `"10 min"`."""
    if isinstance(value, str):
        return time_to_seconds(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    raise ValueError("expected seconds or a duration such as '10 min'")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


class Simulation(_Model):
    """The `simulation` section: the arguments of `SimulationContext`."""

    sim_id: str
    factor: float = 1.0
    random_seed: Optional[int] = None
    logical_start_time: Optional[datetime] = None
    go_live_at: Optional[datetime] = None


class Arrival(_Model):
    """An `arrivals` entry: a distribution, and optionally the task it spawns."""

    dist: Literal["exponential", "normal", "lognormal"]
    rate: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    spawn: Optional[str] = None


class Task(_Model):
    """A `tasks` entry, built with `SimulationContext.task`.

    `payload` is the value of the `finished` event. `id_field` adds the task id to it
    under that key.
    """

    service: str
    resource: str
    payload: Dict[str, Any]
    id_field: Optional[str] = None


class Connector(_Model):
    """An `ingress` entry: a connector type and its constructor arguments."""

    type: str
    config: Dict[str, Any] = Field(default_factory=dict)


class EgressConnector(Connector):
    """An `egress` entry, with the per-provider options of `add_egress`."""

    batch_size: Optional[int] = None
    flush_interval: Optional[float] = None


class Batching(_Model):
    """The `batching` section: the arguments of `with_batching`."""

    batch_size: int
    flush_interval: float
    max_queued_batches: Optional[int] = None
    drain_stall_seconds: Optional[float] = None


class Telemetry(_Model):
    """A `telemetry` entry: metrics published every `interval` simulation seconds.

    `publish` maps a metric name to `<resource>.<stat>`, where the stat is one of
    `capacity`, `in_use`, `queue_length` and `utilization`.
    """

    interval: float
    publish: Dict[str, str]


class Run(_Model):
    """The `run` section."""

    until: Optional[float] = None

    @field_validator("until", mode="before")
    @classmethod
    def _parse_until(cls, value: Any) -> Any:
        return None if value is None else _seconds(value)


class Blueprint(_Model):
    """A whole blueprint file."""

    simulation: Simulation
    resources: Dict[str, CapacityConfig] = Field(default_factory=dict)
    containers: Dict[str, CapacityConfig] = Field(default_factory=dict)
    services: Dict[str, DistributionConfig] = Field(default_factory=dict)
    arrivals: Dict[str, Arrival] = Field(default_factory=dict)
    variables: Dict[str, Any] = Field(default_factory=dict)
    tasks: Dict[str, Task] = Field(default_factory=dict)
    telemetry: List[Telemetry] = Field(default_factory=list)
    ingress: List[Connector] = Field(default_factory=list)
    egress: List[EgressConnector] = Field(default_factory=list)
    batching: Optional[Batching] = None
    run: Run = Field(default_factory=Run)
