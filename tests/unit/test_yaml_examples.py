"""Every YAML example builds the same simulation as its Python twin.

Both sides are built from the files in examples/, then run with the same seed at
factor 0 into capturing sinks. The parameters, the connectors, the records each sink
receives and what each router makes of those records must all match.

Nothing here contacts a broker, database or catalog: connectors are constructed but
never started, and the Iceberg catalog client is replaced before either side builds
one.
"""

import asyncio
import importlib.util
import queue
import random
import sys
from datetime import datetime
from pathlib import Path

import pytest

from dynamic_des import SimulationContext
from dynamic_des.connectors.egress.base import BaseEgress

ROOT = Path(__file__).resolve().parents[2]
START = datetime(2026, 1, 1)

# (Python example, YAML example, expected run.until, simulation seconds to compare)
PAIRS = [
    ("local_example.py", "local.yaml", 60.0, 60),
    ("kafka_example.py", "kafka.yaml", None, 120),
    ("parquet_example.py", "parquet.yaml", 604800.0, 3600),
    ("iceberg_example.py", "iceberg.yaml", 86400.0, 3600),
    ("postgres_example.py", "postgres.yaml", None, 120),
    ("redis_example.py", "redis.yaml", None, 120),
    ("backfill_live_example.py", "backfill_live.yaml", 660.0, 600),
]

# Fields the examples fill from the wall clock with datetime.utcnow, so they differ
# between any two runs, Python or YAML.
WALL_CLOCK_FIELDS = {"order_date", "timestamp"}


class _Capture(BaseEgress):
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


class _Catalog:
    """Stands in for RestCatalog, which contacts the catalog when constructed."""

    def __init__(self, name, **properties):
        self.name = name
        self.properties = properties


@pytest.fixture(autouse=True)
def no_catalog(monkeypatch):
    import pyiceberg.catalog.rest

    monkeypatch.setattr(pyiceberg.catalog.rest, "RestCatalog", _Catalog)


def _python_context(name):
    path = ROOT / "examples" / "declarative" / name
    spec = importlib.util.spec_from_file_location(f"_python_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.app


def _yaml_context(name):
    path = ROOT / "examples" / "yaml" / name
    # A logic module left over from another test would be reused rather than
    # imported again, so each build starts clean.
    for module in [m for m in sys.modules if m.endswith("_logic")]:
        del sys.modules[module]
    return SimulationContext.from_yaml(path)


def _describe(value):
    """A comparable form of a connector attribute.

    Functions from the two files are different objects with the same name, and
    serializers are instances without equality, so both compare by name.
    """
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return [_describe(item) for item in value]
    if isinstance(value, dict):
        return {key: _describe(item) for key, item in value.items()}
    if callable(value) and hasattr(value, "__name__"):
        return f"function {value.__name__}"
    return f"instance of {type(value).__name__}"


def _run(context, until):
    """Runs at factor 0 into one capture per sink, and returns what each received."""
    context.factor = 0.0
    context.random_seed = 42
    context.logical_start_time = START
    context.go_live_at = None
    context._before_run = []
    context._ingress_providers = []
    captures = [_Capture() for _ in context._egress_providers]
    context._egress_providers = captures

    # The Postgres and Redis examples draw from the standard library generator.
    random.seed(42)
    context.run(until=until)
    return [capture.records for capture in captures]


def _mask(record):
    if record.get("path_id") == "system.simulation.lag_seconds":
        # Measured against the wall clock, so it differs between any two runs.
        return {**record, "value": None}
    value = record.get("value")
    if isinstance(value, dict):
        value = {k: v for k, v in value.items() if k not in WALL_CLOCK_FIELDS}
    return {**record, "value": value}


def _routed(provider, records):
    """What the provider's router returns for each record, and the record after it."""
    router = getattr(provider, "path_router", None) or getattr(
        provider, "table_router", None
    )
    if router is None:
        return None
    routed = []
    for record in records:
        copy = dict(record)
        if isinstance(copy.get("value"), dict):
            copy["value"] = dict(copy["value"])
        routed.append((router(copy), copy))
    return routed


@pytest.mark.parametrize(
    "python_name,yaml_name,until,horizon", PAIRS, ids=[p[1] for p in PAIRS]
)
def test_yaml_example_matches_python_example(python_name, yaml_name, until, horizon):
    python_app = _python_context(python_name)
    yaml_app = _yaml_context(yaml_name)

    assert yaml_app._default_until == until
    assert yaml_app.compile_parameters() == python_app.compile_parameters()
    assert (yaml_app.factor, yaml_app.random_seed) == (
        python_app.factor,
        python_app.random_seed,
    )
    assert (yaml_app.logical_start_time is None) == (
        python_app.logical_start_time is None
    )
    assert (yaml_app.go_live_at is None) == (python_app.go_live_at is None)

    for side in ("_ingress_providers", "_egress_providers"):
        python_providers = getattr(python_app, side)
        yaml_providers = getattr(yaml_app, side)
        assert [_describe(vars(p)) for p in yaml_providers] == [
            _describe(vars(p)) for p in python_providers
        ]
        assert [type(p) for p in yaml_providers] == [type(p) for p in python_providers]

    for attribute in (
        "_batch_size",
        "_flush_interval",
        "_egress_batch_sizes",
        "_egress_flush_intervals",
    ):
        assert getattr(yaml_app, attribute) == getattr(python_app, attribute)
    assert [p is None for p in yaml_app._egress_predicates] == [
        p is None for p in python_app._egress_predicates
    ]

    python_sinks = list(python_app._egress_providers)
    yaml_sinks = list(yaml_app._egress_providers)
    python_streams = _run(python_app, horizon)
    yaml_streams = _run(yaml_app, horizon)

    assert any(python_streams), "the Python example published nothing"
    for python_records, yaml_records in zip(python_streams, yaml_streams):
        assert [_mask(r) for r in yaml_records] == [_mask(r) for r in python_records]

    for python_sink, yaml_sink, python_records in zip(
        python_sinks, yaml_sinks, python_streams
    ):
        expected = _routed(python_sink, python_records)
        actual = _routed(yaml_sink, python_records)
        assert actual == expected
