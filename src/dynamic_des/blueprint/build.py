"""Turns a blueprint file into a `SimulationContext`.

Everything is built through the public builder methods, so a blueprint and a Python
script that make the same calls produce the same simulation.
"""

import importlib
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

import simpy
from pydantic import ValidationError

from dynamic_des.blueprint.loader import BlueprintError, SourceMap, read_document
from dynamic_des.blueprint.models import RESOURCE_STATS, Blueprint, Run
from dynamic_des.core.context import SimulationContext
from dynamic_des.core.registry import SimulationRegistry

# Short connector names, mapped to the module, the class and the extra that installs
# what the module imports. Modules are imported only when a blueprint names them, so
# a blueprint that uses Kafka does not need the Postgres driver.
EGRESS_TYPES: Dict[str, Tuple[str, str, str]] = {
    "Console": ("dynamic_des.connectors.egress.local", "ConsoleEgress", ""),
    "Kafka": ("dynamic_des.connectors.egress.kafka", "KafkaEgress", "kafka"),
    "Parquet": (
        "dynamic_des.connectors.egress.storage",
        "ParquetStorageEgress",
        "parquet",
    ),
    "Jsonl": ("dynamic_des.connectors.egress.storage", "JsonlStorageEgress", "parquet"),
    "Iceberg": (
        "dynamic_des.connectors.egress.iceberg",
        "IcebergStorageEgress",
        "iceberg",
    ),
    "Postgres": (
        "dynamic_des.connectors.egress.postgres",
        "PostgresEgress",
        "postgres",
    ),
    "Redis": ("dynamic_des.connectors.egress.redis", "RedisEgress", "redis"),
}
INGRESS_TYPES: Dict[str, Tuple[str, str, str]] = {
    "Local": ("dynamic_des.connectors.ingress.local", "LocalIngress", ""),
    "Kafka": ("dynamic_des.connectors.ingress.kafka", "KafkaIngress", "kafka"),
    "Postgres": (
        "dynamic_des.connectors.ingress.postgres",
        "PostgresIngress",
        "postgres",
    ),
    "Redis": ("dynamic_des.connectors.ingress.redis", "RedisIngress", "redis"),
}


def build(path: Union[str, Path]) -> Tuple[SimulationContext, Run]:
    """Reads, validates and builds a blueprint file.

    Args:
        path: The YAML file.

    Returns:
        The built context, and the `run` section for whoever starts it.

    Raises:
        BlueprintError: If the file is invalid. The message names the file and line.
    """
    data, source = read_document(path)
    try:
        # One `now` for the whole file, so relative times agree with each other.
        blueprint = Blueprint.model_validate(data, context={"now": datetime.now()})
    except ValidationError as exc:
        raise _validation_error(exc, source) from None

    _check_references(blueprint, source)
    context = _build_context(blueprint, source)
    _check_scenario(blueprint, context, source)
    return context, blueprint.run


def _validation_error(exc: ValidationError, source: SourceMap) -> BlueprintError:
    """Lists every validation error, each with its line."""
    messages = []
    for error in exc.errors():
        location = error["loc"]
        where = ".".join(str(part) for part in location) or "top level"
        messages.append(str(source.error(location, f"{where}: {error['msg']}")))
    return BlueprintError("\n".join(messages))


def _check_references(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that every name one section uses is defined in another."""
    for name, task in blueprint.tasks.items():
        if task.service is not None and task.service not in blueprint.services:
            raise source.error(
                ("tasks", name, "service"),
                f"task '{name}' uses service '{task.service}', which is not "
                f"defined under services",
            )
        if task.resource is not None and task.resource not in blueprint.resources:
            raise source.error(
                ("tasks", name, "resource"),
                f"task '{name}' uses resource '{task.resource}', which is not "
                f"defined under resources",
            )

    for name, capacity in blueprint.resources.items():
        for field in ("current_cap", "max_cap"):
            if not float(getattr(capacity, field)).is_integer():
                raise source.error(
                    ("resources", name, field),
                    f"resource '{name}' needs a whole number for {field}",
                )

    for name, arrival in blueprint.arrivals.items():
        if arrival.spawn is not None and arrival.spawn not in blueprint.tasks:
            raise source.error(
                ("arrivals", name, "spawn"),
                f"arrival '{name}' spawns task '{arrival.spawn}', which is not "
                f"defined under tasks",
            )

    for index, egress in enumerate(blueprint.egress):
        if isinstance(egress.when, str) and blueprint.simulation.go_live_at is None:
            raise source.error(
                ("egress", index, "when"),
                f"when: {egress.when} needs simulation.go_live_at",
            )

    for index, entry in enumerate(blueprint.telemetry):
        for metric, reference in (entry.publish or {}).items():
            resource, _, stat = reference.rpartition(".")
            if resource not in blueprint.resources or stat not in RESOURCE_STATS:
                raise source.error(
                    ("telemetry", index, "publish", metric),
                    f"'{reference}' must be <resource>.<stat>, with a resource "
                    f"defined under resources and a stat from "
                    f"{', '.join(RESOURCE_STATS)}",
                )


def _build_context(blueprint: Blueprint, source: SourceMap) -> SimulationContext:
    """Makes the builder calls a Python script would make, in the same order."""
    context = SimulationContext(**blueprint.simulation.model_dump(exclude_unset=True))

    for index, ingress in enumerate(blueprint.ingress):
        provider = _connector(ingress.type, ingress.config, INGRESS_TYPES)
        context.add_ingress(provider(source, ("ingress", index)))

    for index, egress in enumerate(blueprint.egress):
        provider = _connector(egress.type, egress.config, EGRESS_TYPES)
        context.add_egress(
            provider(source, ("egress", index)),
            when=_when(egress.when, blueprint),
            batch_size=egress.batch_size,
            flush_interval=egress.flush_interval,
        )

    if blueprint.batching is not None:
        context.with_batching(**blueprint.batching.model_dump(exclude_none=True))

    for name, capacity in blueprint.resources.items():
        context.add_resource(name, int(capacity.current_cap), int(capacity.max_cap))
    for name, capacity in blueprint.containers.items():
        context.add_container(name, capacity.current_cap, capacity.max_cap)
    for name, value in blueprint.variables.items():
        context.add_variable(name, value)
    for name, dist in blueprint.services.items():
        context.add_service(name, **_set_fields(vars(dist)))
    for name, arrival in blueprint.arrivals.items():
        context.add_arrival(name, **_set_fields(arrival.model_dump(exclude={"spawn"})))

    tasks = {
        name: context.task(service_id=task.service, resource_id=task.resource)(
            _payload(task.payload, task.id_field)
        )
        for name, task in blueprint.tasks.items()
    }
    for name, capacity in blueprint.resources.items():
        for field in ("current_cap", "max_cap"):
            if not float(getattr(capacity, field)).is_integer():
                raise source.error(
                    ("resources", name, field),
                    f"resource '{name}' needs a whole number for {field}",
                )

    for name, arrival in blueprint.arrivals.items():
        if arrival.spawn is not None:
            context.arrival_loop(name)(_spawn_loop(name, tasks[arrival.spawn]))

    for process in blueprint.processes:
        context.add_process(process.function, **process.kwargs)

    for entry in blueprint.telemetry:
        sample = entry.function or _publish_stats(entry.publish or {})
        context.telemetry_loop(entry.interval)(sample)

    if blueprint.scenario:
        steps = sorted(
            ((step.at, step.path, step.value) for step in blueprint.scenario),
            key=lambda step: step[0],
        )
        context.add_process(_run_scenario, steps=steps)

    return context


def _check_scenario(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Checks every scenario path against the registry the run will build.

    The parameters are registered into a scratch registry with the same code the run
    uses, so a path is accepted exactly when `registry.update` would find it. Without
    this a misspelt path is only logged as a warning, at the moment it is due.
    """
    if not blueprint.scenario:
        return

    registry = SimulationRegistry(simpy.Environment())
    registry.register_sim_parameter(context.compile_parameters())

    for index, step in enumerate(blueprint.scenario):
        try:
            current = registry.get(step.path).value
        except KeyError:
            raise source.error(
                ("scenario", index, "path"),
                f"'{step.path}' is not a registry path. Paths look like "
                f"{context.sim_id}.resources.<name>.current_cap or "
                f"{context.sim_id}.arrival.<name>.rate",
            ) from None
        if current is not None and not isinstance(step.value, type(current)):
            try:
                type(current)(step.value)
            except (TypeError, ValueError):
                raise source.error(
                    ("scenario", index, "value"),
                    f"'{step.path}' holds a {type(current).__name__}, and "
                    f"{step.value!r} cannot be converted to one",
                ) from None


def _run_scenario(
    context: SimulationContext, steps: List[Tuple[float, str, Any]]
) -> Generator:
    """Applies each scenario step at its simulation time.

    It waits on the simulation clock, so a scenario repeats exactly and works at
    `factor=0`, where `LocalIngress`, which waits on the wall clock, would not.
    """
    for at, path, value in steps:
        if at > context.env.now:
            yield context.env.timeout(at - context.env.now)
        context.env.registry.update(path, value)


def _when(
    when: Union[str, Callable[[dict], bool], None], blueprint: Blueprint
) -> Optional[Callable[[dict], bool]]:
    """The egress predicate: a `!python` function, or one for `history` or `live`.

    Records carry their logical time as an ISO string in one layout, so comparing the
    text compares the times. The go-live instant is formatted in the same layout and,
    when both times carry a time zone, in the zone of `logical_start_time`.
    """
    if not isinstance(when, str):
        return when

    go_live_at = blueprint.simulation.go_live_at
    start = blueprint.simulation.logical_start_time
    assert go_live_at is not None  # checked in _check_references
    if go_live_at.tzinfo is not None and start is not None and start.tzinfo:
        go_live_at = go_live_at.astimezone(start.tzinfo)
    go_live_iso = go_live_at.isoformat(timespec="milliseconds")

    if when == "history":

        def is_history(record: dict) -> bool:
            return record["timestamp"] < go_live_iso

        return is_history

    def is_live(record: dict) -> bool:
        return record["timestamp"] >= go_live_iso

    return is_live


def _set_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    """Drops unset fields, so the builder's own defaults apply as in Python."""
    return {key: value for key, value in fields.items() if value is not None}


def _connector(
    kind: Union[str, type],
    config: Dict[str, Any],
    types: Dict[str, Tuple[str, str, str]],
) -> Callable[[SourceMap, Tuple[Union[str, int], ...]], Any]:
    """Returns a function that constructs the connector, reporting failures by line.

    `kind` is a short name from `types`, or a class from a `!python` reference.
    """

    def construct(source: SourceMap, location: Tuple[Union[str, int], ...]) -> Any:
        if isinstance(kind, type):
            return _instantiate(kind, config, source, location)
        if kind not in types:
            raise source.error(
                location + ("type",),
                f"unknown connector type '{kind}'. Use one of {', '.join(types)}",
            )
        module_name, class_name, extra = types[kind]
        try:
            cls = getattr(importlib.import_module(module_name), class_name)
        except ImportError as exc:
            hint = (
                f" Install it with `pip install dynamic-des[{extra}]`." if extra else ""
            )
            raise source.error(
                location + ("type",),
                f"{kind} needs a package that is missing: {exc}.{hint}",
            ) from None
        return _instantiate(cls, config, source, location)

    return construct


def _instantiate(
    cls: type,
    config: Dict[str, Any],
    source: SourceMap,
    location: Tuple[Union[str, int], ...],
) -> Any:
    try:
        return cls(**config)
    except TypeError as exc:
        raise source.error(
            location + ("config",), f"{cls.__name__} rejected its config: {exc}"
        ) from None


def _payload(
    payload: Union[Dict[str, Any], Callable[..., Any]], id_field: Union[str, None]
) -> Callable:
    """The task body: a `!python` payload function, or a copy of a mapping payload."""
    if callable(payload):
        return payload

    def body(task_id: int, context: SimulationContext) -> Dict[str, Any]:
        event = dict(payload)
        if id_field is not None:
            event[id_field] = task_id
        return event

    return body


def _spawn_loop(arrival: str, task: Callable) -> Callable:
    """The arrival loop a declarative example writes by hand."""

    def loop(context: SimulationContext):
        task_id = 0
        while True:
            yield context.wait_for_arrival(arrival)
            context.spawn(task(task_id, context))
            task_id += 1

    return loop


def _publish_stats(publish: Dict[str, str]) -> Callable:
    """The telemetry body: publishes each named resource statistic."""

    def sample(context: SimulationContext) -> None:
        for metric, reference in publish.items():
            name, _, stat = reference.rpartition(".")
            res = context.get_resource(name)
            if stat == "capacity":
                value: Any = res.capacity
            elif stat == "in_use":
                value = res.in_use
            elif stat == "queue_length":
                value = len(res.queue.items)
            else:
                value = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
            context.publish(metric, value)

    return sample
