import asyncio
import queue
import uuid
from datetime import datetime

import pyarrow as pa
import pytest

from dynamic_des.connectors.egress.iceberg import IcebergStorageEgress


@pytest.fixture
def namespace(iceberg_catalog):
    """A namespace of its own per test, dropped afterwards."""
    name = f"des_test_{uuid.uuid4().hex[:8]}"
    yield name
    for identifier in iceberg_catalog.list_tables(name):
        iceberg_catalog.drop_table(identifier)
    iceberg_catalog.drop_namespace(name)


async def _drain(egress: IcebergStorageEgress, q: queue.Queue) -> None:
    """Runs the egress until the queue is empty, then cancels it as teardown does."""
    task = asyncio.create_task(egress.run(q))
    for _ in range(100):
        await asyncio.sleep(0.1)
        if q.empty() and not getattr(egress, "active_tasks", 0):
            break
    task.cancel()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_snapshot_count_equals_flush_count(iceberg_catalog, namespace):
    """One commit per flush is the behaviour the buffer size exists to control.

    A writer committing per file instead would leave three times the snapshots here,
    and nothing else in the suite would notice.
    """
    identifier = f"{namespace}.events"
    egress = IcebergStorageEgress(catalog=iceberg_catalog, default_table=identifier)
    q: queue.Queue = queue.Queue()

    for flush in range(3):
        q.put([{"key": f"task-{flush}-{i}", "n": i} for i in range(4)])

    await _drain(egress, q)

    table = iceberg_catalog.load_table(identifier)
    assert len(table.metadata.snapshots) == 3
    assert table.scan().to_arrow().num_rows == 12


@pytest.mark.asyncio
@pytest.mark.integration
async def test_router_writes_two_tables_and_drops_records(iceberg_catalog, namespace):
    """Another engine reads back only what the router kept, split as it routed."""
    events = f"{namespace}.events"
    telemetry = f"{namespace}.telemetry"

    def router(data: dict):
        if data["stream_type"] == "lag":
            return None
        return telemetry if data["stream_type"] == "telemetry" else events

    egress = IcebergStorageEgress(catalog=iceberg_catalog, table_router=router)
    q: queue.Queue = queue.Queue()
    q.put(
        [
            {"stream_type": "event", "key": "a"},
            {"stream_type": "telemetry", "key": "b"},
            {"stream_type": "lag", "key": "dropped"},
            {"stream_type": "event", "key": "c"},
        ]
    )

    await _drain(egress, q)

    event_rows = iceberg_catalog.load_table(events).scan().to_arrow().to_pylist()
    telemetry_rows = iceberg_catalog.load_table(telemetry).scan().to_arrow().to_pylist()

    assert sorted(r["key"] for r in event_rows) == ["a", "c"]
    assert [r["key"] for r in telemetry_rows] == ["b"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_explicit_schema_and_location_survive_the_round_trip(
    iceberg_catalog, namespace
):
    """A pinned timestamp column reads back as a timestamp, at the pinned location."""
    identifier = f"{namespace}.events"
    location = f"s3://warehouse/{namespace}/pinned"
    pinned = pa.schema([("key", pa.string()), ("event_time", pa.timestamp("us"))])

    egress = IcebergStorageEgress(
        catalog=iceberg_catalog,
        default_table=identifier,
        schemas={identifier: pinned},
        locations={identifier: location},
    )
    q: queue.Queue = queue.Queue()
    q.put([{"key": "a", "event_time": datetime(2026, 1, 1, 12, 30)}])

    await _drain(egress, q)

    table = iceberg_catalog.load_table(identifier)
    assert table.location() == location

    arrow = table.scan().to_arrow()
    # Inference would have read this as a string, which is the failure the explicit
    # schema exists to prevent.
    assert pa.types.is_timestamp(arrow.schema.field("event_time").type)
    assert arrow.to_pylist()[0]["key"] == "a"
