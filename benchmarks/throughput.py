"""Throughput benchmark and profile for dynamic-des (issue #5).

This is a measurement tool, not a test. It answers one question: for a run at
``factor=0`` with a fixed seed, how is wall-clock time split between the
simulation core (SimPy scheduling plus the user's generators) and the egress
path (``publish_event``, the Pydantic envelope, the timestamp, batching and the
queue hand-off).

Why ``factor=0``: with any positive factor ``RealtimeEnvironment.step`` sleeps
until wall-clock time catches up with simulation time, so the run is throttled
on purpose and a throughput number would measure the throttle, not the code.

Why a counting sink: the sink has to exercise the real queue hand-off without
adding its own cost to the number. ``CountingEgress`` pulls a batch and adds
``len(batch)`` to a counter, so at a batch size of 500 the sink thread wakes up
about once per 500 records and never dominates. ``ConsoleEgress`` is measured
separately as its own configuration, because logging a line per record is one
of the hypotheses in the issue.

Usage::

    uv run python benchmarks/throughput.py configs   # all configurations
    uv run python benchmarks/throughput.py profile   # cProfile of the baseline
    uv run python benchmarks/throughput.py micro     # Pydantic microbenchmarks
    uv run python benchmarks/throughput.py all
"""

from __future__ import annotations

import asyncio
import cProfile
import io
import logging
import platform
import pstats
import queue
import statistics
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import orjson
from pydantic import BaseModel
from simpy import Environment

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)
from dynamic_des.connectors.egress.base import BaseEgress, extract_dict
from dynamic_des.core.environment import EgressMixIn, IngressMixIn, RegistryMixIn
from dynamic_des.models.schemas import EventPayload

# --------------------------------------------------------------------------
# Workload
# --------------------------------------------------------------------------

SEED = 20260910
UNTIL = 5000.0  # simulation seconds
ARRIVAL_RATE = 10.0  # tasks per simulation second
SERVICE_MEAN = 2.0
SERVICE_STD = 0.4
CAPACITY = 25  # offered load 10 * 2.0 / 25 = 80 percent utilisation
REPEATS = 7


class CountingEgress(BaseEgress):
    """Sink that only counts records, so the measurement is not about the sink."""

    def __init__(self) -> None:
        self.records = 0
        self.batches = 0

    async def run(self, egress_queue: "queue.Queue[Any]") -> None:
        while True:
            try:
                batch = egress_queue.get_nowait()
                self.batches += 1
                self.records += len(batch)
            except queue.Empty:
                await asyncio.sleep(0.05)


class ParkedEgress(BaseEgress):
    """Sink that never consumes, used for the profile only.

    cProfile records every thread that runs while it is enabled, so a consuming
    sink puts the asyncio loop of the egress thread into the same table as the
    simulation and makes the percentages unreadable. This sink parks on an event
    that is never set, so the egress thread contributes one call. Batches pile up
    in the queue instead, which is why the profile uses a flush interval that
    keeps the batch count below ``max_queued_batches``.
    """

    def __init__(self) -> None:
        self.records = 0
        self.batches = 0

    async def run(self, egress_queue: "queue.Queue[Any]") -> None:
        await asyncio.Event().wait()


class CountingConsoleEgress(ConsoleEgress):
    """``ConsoleEgress`` with a record counter, so every configuration reports a rate."""

    def __init__(self) -> None:
        self.records = 0
        self.batches = 0

    async def run(self, egress_queue: "queue.Queue[Any]") -> None:
        while True:
            try:
                batch = egress_queue.get_nowait()
                self.batches += 1
                self.records += len(batch)
                for data in batch:
                    stream = data.pop("stream_type", "unknown")
                    prefix = "[TEL]" if stream == "telemetry" else "[EVT]"
                    logging.getLogger("dynamic_des.connectors.egress.local").info(
                        f"{prefix} {data}"
                    )
            except queue.Empty:
                await asyncio.sleep(0.05)


class PlainEnvironment(Environment, RegistryMixIn, IngressMixIn, EgressMixIn):
    """Same mixins as ``DynamicRealtimeEnvironment`` on a plain SimPy environment.

    Used only to price the real-time bookkeeping that ``RealtimeEnvironment.step``
    still performs when ``factor=0``.
    """

    def __init__(self, logical_start_time: Optional[datetime] = None) -> None:
        self.start_datetime = logical_start_time or datetime.now()
        Environment.__init__(self, initial_time=0)
        self.setup_registry()

    def teardown(self) -> None:
        if hasattr(self, "_event_buffer"):
            self.teardown_egress()


def build_params() -> SimParameter:
    return SimParameter(
        sim_id="Line_A",
        arrival={"standard": DistributionConfig(dist="exponential", rate=ARRIVAL_RATE)},
        service={
            "milling": DistributionConfig(
                dist="normal", mean=SERVICE_MEAN, std=SERVICE_STD
            )
        },
        resources={"lathe": CapacityConfig(current_cap=CAPACITY, max_cap=CAPACITY)},
    )


def attach_workload(env: Any, publish: bool = True, log_events: bool = False) -> None:
    """Attach the arrival, task and telemetry processes to ``env``.

    The sampled values do not depend on the egress configuration, so every
    configuration runs exactly the same sequence of events and publishes exactly
    the same number of records.
    """
    res = DynamicResource(env, "Line_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(SEED))
    log = logging.getLogger("benchmark.workload")

    def work_task(task_id: int, path_id: str):
        task_key = f"task-{task_id}"
        if publish:
            env.publish_event(task_key, {"path_id": path_id, "status": "queued"})
        if log_events:
            log.info("task %s queued", task_key)
        with res.request() as req:
            yield req
            cfg = env.registry.get_config(path_id)
            if publish:
                env.publish_event(task_key, {"path_id": path_id, "status": "started"})
            if log_events:
                log.info("task %s started", task_key)
            yield env.timeout(sampler.sample(cfg))
            if publish:
                env.publish_event(task_key, {"path_id": path_id, "status": "finished"})
            if log_events:
                log.info("task %s finished", task_key)

    def arrival_process():
        arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
        service_path = "Line_A.service.milling"
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(arrival_cfg))
            env.process(work_task(task_id, service_path))
            task_id += 1

    def telemetry_monitor():
        while True:
            if publish:
                env.publish_telemetry("Line_A.resources.lathe.capacity", res.capacity)
                env.publish_telemetry("Line_A.resources.lathe.in_use", res.in_use)
                env.publish_telemetry(
                    "Line_A.resources.lathe.queue_length", len(res.queue.items)
                )
                util = (res.in_use / res.capacity) * 100 if res.capacity else 0
                env.publish_telemetry("Line_A.resources.lathe.utilization", util)
            yield env.timeout(2.0)

    env.process(arrival_process())
    env.process(telemetry_monitor())


# --------------------------------------------------------------------------
# Alternative publish paths, used to price the Pydantic envelope in situ
# --------------------------------------------------------------------------


def publish_event_plain_dict(self, event_key: str, value: Any) -> None:
    """``publish_event`` with the dict built directly, ISO timestamp unchanged."""
    buffer = getattr(self, "_event_buffer", None)
    if buffer is None:
        return
    buffer.append(
        {
            "stream_type": "event",
            "sim_ts": round(self.now, 3),
            "timestamp": (self.start_datetime + timedelta(seconds=self.now)).isoformat(
                timespec="milliseconds"
            ),
            "key": event_key,
            "value": value,
        }
    )
    if len(buffer) >= self.egress_batch_size:
        self._flush_buffer()


def publish_event_plain_dict_epoch(self, event_key: str, value: Any) -> None:
    """As above, but the timestamp is an epoch float rather than an ISO string."""
    buffer = getattr(self, "_event_buffer", None)
    if buffer is None:
        return
    buffer.append(
        {
            "stream_type": "event",
            "sim_ts": round(self.now, 3),
            "timestamp": self._epoch_start + self.now,
            "key": event_key,
            "value": value,
        }
    )
    if len(buffer) >= self.egress_batch_size:
        self._flush_buffer()


def publish_telemetry_plain_dict(self, path_id: str, value: Any) -> None:
    """``publish_telemetry`` with the dict built directly, ISO timestamp unchanged."""
    buffer = getattr(self, "_event_buffer", None)
    if buffer is None:
        return
    buffer.append(
        {
            "stream_type": "telemetry",
            "sim_ts": round(self.now, 3),
            "timestamp": (self.start_datetime + timedelta(seconds=self.now)).isoformat(
                timespec="milliseconds"
            ),
            "path_id": path_id,
            "value": value,
        }
    )
    if len(buffer) >= self.egress_batch_size:
        self._flush_buffer()


# --------------------------------------------------------------------------
# Configurations
# --------------------------------------------------------------------------


_DEVNULL = open("/dev/null", "w")


def configure_logging(level: int) -> None:
    """Send log records to /dev/null with the format the examples use.

    A terminal is far slower than /dev/null and its speed depends on the
    emulator, so the INFO configurations below measure record creation plus
    formatting plus one write, and understate what a demo in a terminal costs.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(_DEVNULL)
    handler.setFormatter(logging.Formatter("%(levelname)s [%(asctime)s] %(message)s"))
    root.addHandler(handler)
    root.setLevel(level)
    logging.getLogger("dynamic_des").setLevel(level)
    logging.getLogger("benchmark.workload").setLevel(level)


def _make_env(realtime: bool) -> Any:
    if realtime:
        return DynamicRealtimeEnvironment(
            factor=0, logical_start_time=datetime(2026, 1, 1)
        )
    return PlainEnvironment(logical_start_time=datetime(2026, 1, 1))


def run_config(
    name: str,
    *,
    egress: bool = True,
    publish: bool = True,
    realtime: bool = True,
    console_sink: bool = False,
    parked_sink: bool = False,
    log_level: int = logging.WARNING,
    publish_event_override: Optional[Callable] = None,
    publish_telemetry_override: Optional[Callable] = None,
    lag_monitor_interval: Optional[float] = 1.0,
    flush_interval: float = 1.0,
    drain_stall_seconds: float = 30.0,
    log_events: bool = False,
) -> Tuple[float, int, int, int]:
    """Run one configuration once. Returns (run seconds, records published)."""
    configure_logging(log_level)

    env = _make_env(realtime)
    env.registry.register_sim_parameter(build_params())

    sink: Any = None
    if egress:
        if parked_sink:
            sink = ParkedEgress()
        elif console_sink:
            sink = CountingConsoleEgress()
        else:
            sink = CountingEgress()
        env.setup_egress(
            [sink],
            lag_monitor_interval=lag_monitor_interval,
            flush_interval=flush_interval,
            drain_stall_seconds=drain_stall_seconds,
        )

    env._epoch_start = env.start_datetime.timestamp()
    if publish_event_override is not None:
        env.publish_event = publish_event_override.__get__(env, type(env))
    if publish_telemetry_override is not None:
        env.publish_telemetry = publish_telemetry_override.__get__(env, type(env))

    attach_workload(env, publish=publish, log_events=log_events)

    started = time.perf_counter()
    try:
        env.run(until=UNTIL)
    finally:
        elapsed = time.perf_counter() - started
        # Batches still queued when the run ends show whether the sink kept up.
        backlog = sum(q.qsize() for q in getattr(env, "egress_queues", []))
        env.teardown()

    return elapsed, backlog, getattr(sink, "records", 0), getattr(sink, "batches", 0)


CONFIGS: List[Tuple[str, Dict[str, Any]]] = [
    # 1. simulation core alone
    ("A. core only, workload publishes nothing", dict(egress=False, publish=False)),
    # 2. core plus the failed hasattr probe, no egress configured
    ("B. core, publish calls return at the probe", dict(egress=False, publish=True)),
    # 3. as shipped
    ("C. baseline, counting sink, WARNING logs", dict()),
    # 4. prices the real-time bookkeeping that remains at factor=0
    ("D. baseline on plain simpy.Environment", dict(realtime=False)),
    # 5. Pydantic removed from the event path only
    (
        "E. events as plain dicts, ISO timestamp kept",
        dict(publish_event_override=publish_event_plain_dict),
    ),
    # 6. Pydantic removed from both paths
    (
        "F. events and telemetry as plain dicts",
        dict(
            publish_event_override=publish_event_plain_dict,
            publish_telemetry_override=publish_telemetry_plain_dict,
        ),
    ),
    # 7. also drops the per-event datetime work
    (
        "G. F plus epoch float instead of ISO string",
        dict(
            publish_event_override=publish_event_plain_dict_epoch,
            publish_telemetry_override=publish_telemetry_plain_dict,
        ),
    ),
    # 8. prices the automatic lag telemetry
    ("H. baseline with the lag monitor off", dict(lag_monitor_interval=0)),
    # 9. hypothesis 1 from the issue, logging in the sink thread
    (
        "I. ConsoleEgress sink, INFO logs",
        dict(console_sink=True, log_level=logging.INFO),
    ),
    # 10. hypothesis 1 again, logging on the main thread as the examples do
    (
        "J. baseline plus INFO logging in the workload",
        dict(log_level=logging.INFO, log_events=True),
    ),
    # 11. flush_interval is in simulation seconds, so at factor=0 it fires
    #     constantly and batch_size never governs. This removes it.
    ("K. baseline, flush_interval raised to 1e9", dict(flush_interval=1e9)),
    # 12. K plus the cheap publish path, the combined best case
    (
        "L. K plus plain dicts and epoch timestamps",
        dict(
            flush_interval=1e9,
            publish_event_override=publish_event_plain_dict_epoch,
            publish_telemetry_override=publish_telemetry_plain_dict,
        ),
    ),
    # 13. egress wired up but nothing published, so this prices the egress
    #     background SimPy processes on their own
    (
        "M. egress configured, nothing published",
        dict(publish=False, lag_monitor_interval=0),
    ),
    # 14. every cheap change together: plain dicts, epoch timestamps and no
    #     real-time bookkeeping. This is the ceiling without touching SimPy.
    (
        "N. G on plain simpy.Environment",
        dict(
            realtime=False,
            publish_event_override=publish_event_plain_dict_epoch,
            publish_telemetry_override=publish_telemetry_plain_dict,
        ),
    ),
]


def cmd_configs() -> Dict[str, Tuple[float, float, int]]:
    print(header())
    print(
        f"\nWorkload: until={UNTIL:.0f} simulation seconds, arrival rate "
        f"{ARRIVAL_RATE}/s, service normal(mean={SERVICE_MEAN}, std={SERVICE_STD}), "
        f"capacity {CAPACITY}, seed {SEED}, {REPEATS} repeats per configuration."
    )
    print("Times cover env.run() only; teardown and the final drain are excluded.")
    print("backlog is the batches still queued when the run ends.\n")
    print(
        f"{'configuration':<46} {'median s':>9} {'min s':>8} {'records':>9}"
        f" {'rec/s':>9} {'batches':>8} {'backlog':>8}"
    )
    print("-" * 102)
    results: Dict[str, Tuple[float, float, int]] = {}
    for name, kwargs in CONFIGS:
        times: List[float] = []
        records = batches = backlog = 0
        for _ in range(REPEATS):
            elapsed, backlog, records, batches = run_config(name, **kwargs)
            times.append(elapsed)
        median = statistics.median(times)
        results[name] = (median, min(times), records)
        rate = records / median if records else float("nan")
        print(
            f"{name:<46} {median:>9.3f} {min(times):>8.3f} {records:>9d}"
            f" {rate:>9.0f} {batches:>8d} {backlog:>8d}"
        )
    return results


# --------------------------------------------------------------------------
# Profile
# --------------------------------------------------------------------------


PROFILE_CONFIG = dict(parked_sink=True, flush_interval=1e9, drain_stall_seconds=0.1)


def cmd_profile(top: int = 25) -> None:
    print(header())
    print(
        "\ncProfile of the baseline publish path with a sink that never consumes,"
        "\nso only the simulation thread appears in the table."
        "\nAbsolute times are inflated by the profiler; read the shares, not the seconds.\n"
    )
    profiler = cProfile.Profile()
    profiler.enable()
    # PROFILE_CONFIG is a heterogeneous mapping, so mypy cannot match it against
    # run_config's typed keyword parameters. The call is correct at runtime.
    elapsed, backlog, records, _batches = run_config("profile", **PROFILE_CONFIG)  # type: ignore[arg-type]
    profiler.disable()

    print(
        f"run seconds under the profiler: {elapsed:.3f}; "
        f"{backlog} batches of {500} records were buffered and never consumed."
    )
    del records
    for sort_key in ("cumulative", "tottime"):
        stream = io.StringIO()
        stats = pstats.Stats(profiler, stream=stream)
        stats.sort_stats(sort_key).print_stats(top)
        print(f"\n===== sorted by {sort_key} =====")
        print(stream.getvalue())

    print("===== core against egress, from cumulative time =====")
    stats = pstats.Stats(profiler, stream=io.StringIO())
    by_name: Dict[str, Tuple[int, float, float]] = {}
    # pstats.Stats exposes `stats` at runtime but does not declare it.
    for (filename, _lineno, func), (_cc, nc, tt, ct, _cal) in stats.stats.items():  # type: ignore[attr-defined]
        by_name[f"{filename.rsplit('/', 1)[-1]}:{func}"] = (nc, tt, ct)

    total = by_name.get("core.py:run", (0, 0.0, 0.0))[2]
    for label in (
        "core.py:run",
        "environment.py:publish_event",
        "environment.py:publish_telemetry",
        "environment.py:_flush_buffer",
        "environment.py:_get_iso_timestamp",
        "main.py:__init__",
        "main.py:model_dump",
    ):
        if label in by_name:
            nc, tt, ct = by_name[label]
            share = 100.0 * ct / total if total else 0.0
            print(
                f"{label:<40} calls={nc:>8d} tottime={tt:>7.3f} "
                f"cumtime={ct:>7.3f} ({share:>5.1f}% of env.run)"
            )
    egress = sum(
        by_name.get(label, (0, 0.0, 0.0))[2]
        for label in (
            "environment.py:publish_event",
            "environment.py:publish_telemetry",
        )
    )
    print(
        f"\negress path (publish_event + publish_telemetry, including the flush "
        f"they trigger): {100.0 * egress / total:.1f}% of env.run cumulative time."
    )
    print(
        "The remainder is the simulation core: SimPy stepping, event resumption, "
        "resource handling and sampling."
    )


# --------------------------------------------------------------------------
# Microbenchmarks
# --------------------------------------------------------------------------


def _time_loop(fn: Callable[[], Any], iterations: int = 200_000) -> float:
    """Nanoseconds per call, best of five."""
    best = float("inf")
    for _ in range(5):
        started = time.perf_counter()
        for _ in range(iterations):
            fn()
        best = min(best, time.perf_counter() - started)
    return best / iterations * 1e9


def cmd_micro() -> None:
    print(header())
    print("\nMicrobenchmarks, nanoseconds per call, best of five runs.\n")

    start = datetime(2026, 1, 1)
    now = 1234.5678
    key = "task-12345"
    value = {"path_id": "Line_A.service.milling", "status": "finished"}
    record = {
        "stream_type": "event",
        "sim_ts": 1234.568,
        "timestamp": "2026-01-01T00:20:34.567",
        "key": key,
        "value": value,
    }

    class TaskEvent(BaseModel):
        """Stands in for the Pydantic value that kafka_example.py publishes."""

        path_id: str
        status: str

    model_value = TaskEvent(path_id="Line_A.service.milling", status="finished")

    def iso() -> str:
        return (start + timedelta(seconds=now)).isoformat(timespec="milliseconds")

    cases: List[Tuple[str, Callable[[], Any]]] = [
        (
            "EventPayload construct only",
            lambda: EventPayload(
                key=key,
                value=value,
                sim_ts=round(now, 3),
                timestamp="2026-01-01T00:20:34.567",
            ),
        ),
        (
            "EventPayload construct plus model_dump(mode=json)",
            lambda: EventPayload(
                key=key,
                value=value,
                sim_ts=round(now, 3),
                timestamp="2026-01-01T00:20:34.567",
            ).model_dump(mode="json"),
        ),
        (
            "plain dict with the same five fields",
            lambda: {
                "stream_type": "event",
                "sim_ts": round(now, 3),
                "timestamp": "2026-01-01T00:20:34.567",
                "key": key,
                "value": value,
            },
        ),
        ("_get_iso_timestamp (datetime + timedelta + isoformat)", iso),
        ("epoch float timestamp (start + now)", lambda: 1767225600.0 + now),
        ("hasattr(self, '_event_buffer') probe", lambda: hasattr(sys, "_event_buffer")),
        ("round(now, 3)", lambda: round(now, 3)),
        (
            "extract_dict on a dict (isinstance short circuit)",
            lambda: extract_dict(record),
        ),
        ("orjson.dumps of one finished record", lambda: orjson.dumps(record)),
        (
            "EventPayload construct plus dump, value is a model",
            lambda: EventPayload(
                key=key,
                value=model_value,
                sim_ts=round(now, 3),
                timestamp="2026-01-01T00:20:34.567",
            ).model_dump(mode="json"),
        ),
    ]

    baseline_name = "plain dict with the same five fields"
    results: Dict[str, float] = {}
    for name, fn in cases:
        ns = _time_loop(fn)
        results[name] = ns
        print(f"{name:<56} {ns:>8.1f} ns")

    full = results["EventPayload construct plus model_dump(mode=json)"]
    plain = results[baseline_name]
    print(
        f"\nPydantic envelope costs {full - plain:.0f} ns more per event than "
        f"building the dict directly ({full / plain:.1f}x)."
    )


def header() -> str:
    return (
        f"machine: {platform.machine()}, {platform.platform()}\n"
        f"python: {sys.version.split()[0]} ({sys.implementation.name})"
    )


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    if command in ("configs", "all"):
        cmd_configs()
    if command in ("micro", "all"):
        print()
        cmd_micro()
    if command in ("profile", "all"):
        print()
        cmd_profile()
