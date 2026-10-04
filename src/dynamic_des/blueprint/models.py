"""Pydantic models for a blueprint file.

Every model forbids unknown keys, so a misspelt key is reported rather than ignored.
Distributions and capacities reuse `DistributionConfig` and `CapacityConfig`, the
dataclasses the builder already registers. Fields that take Python objects are
filled by `!python` references and checked here, so a wrong reference fails when the
file is loaded rather than during the run.
"""

import inspect
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Type, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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


def _generator_function(value: Any) -> Any:
    if not inspect.isgeneratorfunction(value):
        raise ValueError(
            f"{value!r} must be a generator function, one that uses `yield`"
        )
    return value


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

    `payload` is the value of the `finished` event: a mapping, or a `!python`
    function called as `payload(task_id, context)`. `id_field` adds the task id to a
    mapping payload under that key.
    """

    service: str
    resource: str
    payload: Union[Dict[str, Any], Callable[..., Any]]
    id_field: Optional[str] = None

    @model_validator(mode="after")
    def _id_field_needs_a_mapping(self) -> "Task":
        if self.id_field is not None and not isinstance(self.payload, dict):
            raise ValueError("id_field applies only to a mapping payload")
        return self


class Process(_Model):
    """A `processes` entry: a generator function and extra keyword arguments.

    The function is called as `function(context, **kwargs)` when the run starts.
    """

    function: Callable[..., Any]
    kwargs: Dict[str, Any] = Field(default_factory=dict)

    _check_function = field_validator("function")(_generator_function)


class Connector(_Model):
    """An `ingress` entry: a connector type and its constructor arguments.

    `type` is a short name such as `Kafka`, or a `!python` class for a connector
    from another package.
    """

    type: Union[str, Type[Any]]
    config: Dict[str, Any] = Field(default_factory=dict)


class EgressConnector(Connector):
    """An `egress` entry, with the per-provider options of `add_egress`."""

    when: Optional[Callable[[dict], bool]] = None
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
    `capacity`, `in_use`, `queue_length` and `utilization`. `function` is a `!python`
    function called as `function(context)` instead. Give one or the other.
    """

    interval: float
    publish: Optional[Dict[str, str]] = None
    function: Optional[Callable[..., Any]] = None

    @model_validator(mode="after")
    def _one_source(self) -> "Telemetry":
        if (self.publish is None) == (self.function is None):
            raise ValueError("give either publish or function")
        return self


class ScenarioStep(_Model):
    """A `scenario` entry: set the registry `path` to `value` at simulation time `at`.

    `at` is seconds from the start of the run, or a duration such as `"10 min"`.
    """

    at: float
    path: str
    value: Any

    @field_validator("at", mode="before")
    @classmethod
    def _parse_at(cls, value: Any) -> Any:
        return _seconds(value)


class Run(_Model):
    """The `run` section.

    `before` lists `!python` functions called with no arguments before the run
    starts, for setup such as creating topics or tables.
    """

    until: Optional[float] = None
    before: List[Callable[[], Any]] = Field(default_factory=list)

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
    processes: List[Process] = Field(default_factory=list)
    scenario: List[ScenarioStep] = Field(default_factory=list)
    ingress: List[Connector] = Field(default_factory=list)
    egress: List[EgressConnector] = Field(default_factory=list)
    batching: Optional[Batching] = None
    run: Run = Field(default_factory=Run)

    @field_validator("processes", mode="before")
    @classmethod
    def _bare_functions(cls, value: Any) -> Any:
        """Lets a process be written as a bare `!python` reference."""
        if isinstance(value, list):
            return [
                item if isinstance(item, dict) else {"function": item} for item in value
            ]
        return value
