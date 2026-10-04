"""Every YAML example builds, runs at factor 0 and produces the records it describes.

Each file in examples/yaml/ is built as `ddes run` builds it, with the
environment variables it reads left unset so the defaults apply. The connectors are
checked as constructed, then replaced by capturing sinks for a short run at factor 0.
The captured records are passed to the real connector code where that needs no
server: Parquet writes to a temporary folder, Iceberg appends to a fake catalog, and
Postgres and Redis write through a mocked client.

Nothing here contacts a broker, database or catalog.
"""

import asyncio
import queue
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import fs

from dynamic_des import SimulationContext
from dynamic_des.connectors.egress.base import BaseEgress
from dynamic_des.connectors.egress.storage import _build_filesystem

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples" / "yaml"
START = datetime(2026, 1, 1)

# Every variable an example reads, cleared so each test sees the file's defaults.
VARIABLES = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "DEST_PATH",
    "PARQUET_FILESYSTEM",
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "S3_REGION",
    "ICEBERG_URI",
    "ICEBERG_WAREHOUSE",
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)


class _Capture(BaseEgress):
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _build(name):
    # A logic module left over from another test would be reused rather than
    # imported again, so each build starts clean.
    for module in [m for m in sys.modules if m.endswith("_logic")]:
        del sys.modules[module]
    return SimulationContext.from_yaml(EXAMPLES / name)


def _run(context, until, start=START):
    """Runs at factor 0 into one capture per sink, and returns what each received.

    The `when` predicates are kept, so each capture receives what its sink would.
    """
    context.factor = 0.0
    context.logical_start_time = start
    # The run stays unpaced, so a live tail costs no real time.
    context.go_live_at = None
    context._before_run = []
    context._ingress_providers = []
    context._egress_providers = [_Capture() for _ in context._egress_providers]
    context.run(until=until)
    return [capture.records for capture in context._egress_providers]


def _events(records):
    return [r for r in records if r["stream_type"] == "event"]


def _finished(records):
    """The values of the finished events, which are the task payloads."""
    return [
        r["value"]
        for r in _events(records)
        if r["value"].get("status") not in ("queued", "started")
    ]


def _telemetry_paths(records):
    return {r["path_id"] for r in records if r["stream_type"] == "telemetry"}


async def _drain(egress, records):
    """Runs an egress over one batch until it has handled it."""
    egress_queue: queue.Queue = queue.Queue()
    egress_queue.put(records)
    task = asyncio.create_task(egress.run(egress_queue))
    for _ in range(100):
        await asyncio.sleep(0.01)
        if egress_queue.empty():
            break
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def test_every_example_is_plain_yaml():
    """Only the advanced example references Python."""
    plain = sorted(EXAMPLES.glob("*.yaml"))
    assert len(plain) == 7
    for path in plain:
        assert "!python" not in path.read_text(encoding="utf-8"), path.name
    assert not list(EXAMPLES.glob("*.py"))


def test_local():
    app = _build("local.yaml")
    assert app._default_until == 60.0
    [records] = _run(app, 60)

    finished = _finished(records)
    assert finished
    # local.yaml has no seed and two lathes, so a later part can finish first
    assert {"event_type": "part_produced", "quality": "A", "part_id": 0} in finished
    assert {"Factory_A.utilization", "Factory_A.queue_length"} <= _telemetry_paths(
        records
    )


def test_kafka():
    app = _build("kafka.yaml")
    [ingress] = app._ingress_providers
    [egress] = app._egress_providers
    assert app._default_until is None
    assert ingress.topic == "sim-config"
    assert ingress.bootstrap_servers == "localhost:9092"
    assert (egress.event_topic, egress.telemetry_topic) == (
        "sim-events",
        "sim-telemetry",
    )
    assert egress.producer_config["bootstrap_servers"] == "localhost:9092"
    # No router, so the egress creates both topics when it starts.
    assert egress.topic_router is None

    [records] = _run(app, 120)
    finished = _finished(records)
    assert finished
    assert all(
        value == {"path_id": "Line_A.service.milling", "status": "finished"}
        for value in finished
    )
    assert {
        "Line_A.lathe.capacity",
        "Line_A.lathe.in_use",
        "Line_A.lathe.queue_length",
        "Line_A.lathe.utilization",
    } <= _telemetry_paths(records)
    capacity = [
        r["value"] for r in records if r.get("path_id") == "Line_A.lathe.capacity"
    ]
    assert set(capacity) == {1}


def test_kafka_reads_the_broker_from_the_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    app = _build("kafka.yaml")
    assert app._egress_providers[0].producer_config["bootstrap_servers"] == (
        "broker:29092"
    )


def test_parquet(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    app = _build("parquet.yaml")
    [egress] = app._egress_providers
    assert app._default_until == 604800.0
    assert egress.default_path == "data/events.parquet"
    assert egress.path_router is None
    assert isinstance(_build_filesystem(egress.filesystem, fs), fs.LocalFileSystem)
    assert (app._batch_size, app._flush_interval) == (5000, 86400)

    [records] = _run(app, 3600)
    egress.filesystem = _build_filesystem(egress.filesystem, fs)
    egress._write_batch(records, pa, pq)

    # The folder did not exist, and the first write created it.
    [chunk] = (tmp_path / "data").glob("events_*.parquet")
    rows = pq.read_table(chunk).to_pylist()
    assert len(rows) == len(_events(records))
    assert set(rows[0]) == {
        "stream_type",
        "sim_ts",
        "timestamp",
        "key",
        "path_id",
        "status",
    }
    assert {row["status"] for row in rows} == {"queued", "started", "finished"}


def test_parquet_on_s3(monkeypatch):
    """The variables in the file's header switch the same file to S3."""
    monkeypatch.setenv("PARQUET_FILESYSTEM", "s3")
    monkeypatch.setenv("DEST_PATH", "odctl-dev/history")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:8333")
    monkeypatch.setenv("S3_ACCESS_KEY", "user")
    monkeypatch.setenv("S3_SECRET_KEY", "password")
    [egress] = _build("parquet.yaml")._egress_providers

    assert egress.default_path == "odctl-dev/history/events.parquet"
    built = _build_filesystem(egress.filesystem, fs)
    assert isinstance(built, fs.S3FileSystem)
    options = built.__reduce__()[1][0]
    assert (options["endpoint_override"], options["scheme"]) == (
        "localhost:8333",
        "http",
    )
    assert (options["access_key"], options["secret_key"]) == ("user", "password")


class _Table:
    def __init__(self, schema):
        self._schema = schema
        self.appended = []

    def schema(self):
        return MagicMock(as_arrow=lambda: self._schema)

    def append(self, table):
        self.appended.append(table)


class _Catalog:
    """Stands in for the REST catalog: creates tables in memory."""

    def __init__(self):
        self.tables = {}

    def create_namespace_if_not_exists(self, namespace):
        pass

    def create_table_if_not_exists(self, identifier, schema, location=None):
        return self.tables.setdefault(identifier, _Table(schema))


def test_iceberg():
    app = _build("iceberg.yaml")
    [egress] = app._egress_providers
    assert app._default_until == 86400.0
    assert app._egress_batch_sizes == [200000]
    # The catalog is a mapping until the first write, so loading contacts nothing.
    assert egress.catalog is None
    assert egress.catalog_properties == {
        "name": "odctl",
        "type": "rest",
        "uri": "http://localhost:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://localhost:8333",
        "s3.access-key-id": "user",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    }
    assert egress.default_table == "simulation.events"
    assert egress.table_router is None
    assert egress.schemas["simulation.events"] == pa.schema(
        [
            ("sim_ts", pa.float64()),
            ("timestamp", pa.timestamp("us")),
            ("key", pa.string()),
            ("path_id", pa.string()),
            ("status", pa.string()),
        ]
    )

    [records] = _run(app, 3600)
    egress.catalog = _Catalog()
    egress._write_batch(records, pa)

    [table] = egress.catalog.tables["simulation.events"].appended
    assert table.num_rows == len(_events(records))
    rows = table.to_pylist()
    assert all(isinstance(row["timestamp"], datetime) for row in rows)
    assert rows[0]["timestamp"] >= START
    assert {row["status"] for row in rows} == {"queued", "started", "finished"}


# How information_schema names the SQL types the examples use.
DATA_TYPES = {"TIMESTAMP": "timestamp without time zone", "INT": "integer"}


@pytest.fixture
def postgres():
    """A mocked asyncpg pool whose tables have the columns the examples create.

    Set `egresses` to the egresses under test, so a column lookup finds the table.
    """
    conn = AsyncMock()
    acquire = AsyncMock()
    acquire.__aenter__.return_value = conn
    pool = MagicMock()
    pool.acquire.return_value = acquire
    state = SimpleNamespace(created={}, conn=conn, egresses=[])

    async def execute(sql, *args):
        if sql.startswith("CREATE TABLE IF NOT EXISTS "):
            state.created[sql.split()[5]] = sql

    async def fetch(sql, table_name):
        egress = next(e for e in state.egresses if e.table_name == table_name)
        columns, _ = egress.tables[table_name]
        return [
            {"column_name": column, "data_type": DATA_TYPES.get(sql_type, sql_type)}
            for column, sql_type in columns.items()
        ]

    conn.execute.side_effect = execute
    conn.fetch.side_effect = fetch
    with patch(
        "dynamic_des.connectors.egress.postgres.asyncpg.create_pool",
        new=AsyncMock(return_value=pool),
    ):
        yield state


def _written(conn, table):
    """The rows the egress inserted into `table`, as dictionaries."""
    rows = []
    for call in conn.executemany.call_args_list:
        query, values = call.args
        if query.startswith(f"INSERT INTO {table} ("):
            columns = query.split("(", 1)[1].split(")", 1)[0].split(", ")
            rows.extend(dict(zip(columns, row)) for row in values)
    return rows


@pytest.mark.asyncio
async def test_postgres(postgres):
    app = _build("postgres.yaml")
    [ingress] = app._ingress_providers
    [egress] = app._egress_providers
    assert ingress.table_name == "simulation_params"
    assert egress.table_name == "orders"
    postgres.egresses = [egress]

    [records] = await asyncio.to_thread(_run, app, 120)
    await _drain(egress, records)

    assert postgres.created["orders"] == (
        "CREATE TABLE IF NOT EXISTS orders (order_id INT, customer_id INT, "
        "total_amount REAL, status TEXT, timestamp TIMESTAMP, PRIMARY KEY (order_id))"
    )
    rows = _written(postgres.conn, "orders")
    assert len(rows) == len(_finished(records)) > 0
    assert [row["order_id"] for row in rows] == list(range(len(rows)))
    assert {(r["customer_id"], r["total_amount"], r["status"]) for r in rows} == {
        (42, 59.97, "pending")
    }
    # The ISO time of the record is converted for the TIMESTAMP column.
    assert all(isinstance(row["timestamp"], datetime) for row in rows)


@pytest.mark.asyncio
async def test_redis():
    app = _build("redis.yaml")
    [ingress] = app._ingress_providers
    [egress] = app._egress_providers
    assert ingress.channel_name == "simulation_params"
    assert egress.stream_name == "events"

    [records] = await asyncio.to_thread(_run, app, 120)
    finished = _finished(records)
    assert finished
    # The task has no service or resource, so it emits no queued or started event.
    assert len(finished) == len(_events(records))
    assert finished[:2] == [
        {"__stream__": "part_events", "type": "A", "status": "arrived", "part_id": 0},
        {"__stream__": "part_events", "type": "A", "status": "arrived", "part_id": 1},
    ]

    client = MagicMock()
    pipe = client.pipeline.return_value
    pipe.execute = AsyncMock()
    with patch(
        "dynamic_des.connectors.egress.redis.redis.from_url", return_value=client
    ):
        await _drain(egress, records)

    streams = [call.args[0] for call in pipe.xadd.call_args_list]
    assert streams.count("part_events") == len(finished)
    assert set(streams) == {"part_events", "events"}


def test_backfill_live():
    app = _build("backfill_live.yaml")
    history, live = app._egress_providers
    assert app._default_until == 660.0
    assert (app.go_live_at - app.logical_start_time).total_seconds() == 600
    assert history.default_path == "data/backfill/events.parquet"
    assert live.producer_config["bootstrap_servers"] == "localhost:9092"
    assert (app._batch_size, app._flush_interval) == (2000, 10.0)

    go_live_iso = app.go_live_at.isoformat(timespec="milliseconds")
    history_records, live_records = _run(app, 660, start=app.logical_start_time)

    assert history_records and live_records
    assert all(r["timestamp"] < go_live_iso for r in history_records)
    assert all(r["timestamp"] >= go_live_iso for r in live_records)
    assert max(r["sim_ts"] for r in history_records) < 600
    assert min(r["sim_ts"] for r in live_records) >= 600


@pytest.mark.asyncio
async def test_advanced_postgres_orders(postgres):
    app = _build("advanced/postgres_orders.yaml")
    orders, items = app._egress_providers
    assert (orders.table_name, items.table_name) == ("orders", "order_items")
    assert app.random_seed == 42
    postgres.egresses = [orders, items]

    [first, second] = await asyncio.to_thread(_run, app, 120)
    assert first == second, "both sinks receive every record"
    await _drain(orders, first)
    await _drain(items, first)

    assert set(postgres.created) == {"orders", "order_items"}
    order_rows = _written(postgres.conn, "orders")
    item_rows = _written(postgres.conn, "order_items")
    assert order_rows and item_rows
    assert [row["order_id"] for row in order_rows] == list(
        range(1, len(order_rows) + 1)
    )

    for order in order_rows:
        lines = [item for item in item_rows if item["order_id"] == order["order_id"]]
        assert 1 <= len(lines) <= 5
        for item in lines:
            assert 10.0 <= item["unit_price"] <= 50.0
            assert 1 <= item["quantity"] <= 3
        expected = sum(item["unit_price"] * item["quantity"] for item in lines)
        assert order["total_amount"] == round(expected, 2)
        assert 1 <= order["customer_id"] <= 100
        assert order["status"] == "pending"


def test_advanced_postgres_orders_repeats_with_the_seed():
    def run():
        [records, _] = _run(_build("advanced/postgres_orders.yaml"), 60)
        return [r["value"] for r in _events(records)]

    assert run() == run()
