import pytest

# Attempt to import PyArrow, skip tests if not installed
try:
    import pyarrow as pa

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

from dynamic_des.connectors.egress.iceberg import IcebergStorageEgress

pytestmark = pytest.mark.skipif(
    not HAS_PYARROW, reason="pyarrow is required for the Iceberg egress tests"
)


class FakeSchema:
    """Stands in for a pyiceberg Schema, which the connector reads back as Arrow."""

    def __init__(self, arrow_schema):
        self._arrow_schema = arrow_schema

    def as_arrow(self):
        return self._arrow_schema


class FakeTable:
    """Records every append, so a test can count commits and inspect what was sent."""

    def __init__(self, identifier, arrow_schema, location):
        self.identifier = identifier
        self.location = location
        self._schema = FakeSchema(arrow_schema)
        self.appended: list = []

    def schema(self):
        return self._schema

    def append(self, df):
        self.appended.append(df)


class FakeCatalog:
    """A pyiceberg catalog reduced to the three calls the connector makes."""

    def __init__(self):
        self.namespaces: list = []
        self.tables: dict = {}
        self.create_calls: list = []

    def create_namespace_if_not_exists(self, namespace):
        self.namespaces.append(namespace)

    def create_table_if_not_exists(self, identifier, schema, location=None):
        self.create_calls.append((identifier, schema, location))
        if identifier not in self.tables:
            self.tables[identifier] = FakeTable(identifier, schema, location)
        return self.tables[identifier]


def test_requires_a_destination():
    """Without a table or a router the connector has nowhere to write."""
    with pytest.raises(ValueError, match="default_table or table_router"):
        IcebergStorageEgress(catalog=FakeCatalog())


def test_identifier_must_name_a_namespace():
    """A bare table name is not an Iceberg identifier, and says so before writing."""
    egress = IcebergStorageEgress(catalog=FakeCatalog(), default_table="events")

    with pytest.raises(ValueError, match="names no namespace"):
        egress._write_batch([{"key": "a"}], pa)


def test_router_splits_records_across_tables():
    """Each target table gets its own append, holding only its own records."""
    catalog = FakeCatalog()

    def router(data: dict):
        if data["stream_type"] == "telemetry":
            return "sim.telemetry"
        return "sim.events"

    egress = IcebergStorageEgress(catalog=catalog, table_router=router)
    egress._write_batch(
        [
            {"stream_type": "event", "key": "a"},
            {"stream_type": "telemetry", "key": "b"},
            {"stream_type": "event", "key": "c"},
        ],
        pa,
    )

    events = catalog.tables["sim.events"]
    telemetry = catalog.tables["sim.telemetry"]
    assert [r["key"] for r in events.appended[0].to_pylist()] == ["a", "c"]
    assert [r["key"] for r in telemetry.appended[0].to_pylist()] == ["b"]
    assert catalog.namespaces == ["sim", "sim"]


def test_router_returning_none_drops_the_record():
    """A dropped record reaches no table at all."""
    catalog = FakeCatalog()

    def router(data: dict):
        return None if data["key"] == "drop-me" else "sim.events"

    egress = IcebergStorageEgress(catalog=catalog, table_router=router)
    egress._write_batch([{"key": "drop-me"}, {"key": "keep-me"}], pa)

    rows = catalog.tables["sim.events"].appended[0].to_pylist()
    assert [r["key"] for r in rows] == ["keep-me"]


def test_every_record_dropped_creates_no_table():
    """A batch with nothing to write must not reach the catalog."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, table_router=lambda data: None)

    egress._write_batch([{"key": "a"}, {"key": "b"}], pa)

    assert catalog.create_calls == []
    assert catalog.namespaces == []


def test_one_append_per_batch_is_one_commit_per_flush():
    """Snapshot count follows flush count, which is the reason for the large buffer."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.events")

    for _ in range(3):
        egress._write_batch([{"key": "a"}, {"key": "b"}], pa)

    table = catalog.tables["sim.events"]
    assert len(table.appended) == 3
    assert [t.num_rows for t in table.appended] == [2, 2, 2]


def test_the_table_is_created_once_and_reused():
    """A cached handle keeps a batch to one append rather than a catalog round trip."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.events")

    egress._write_batch([{"key": "a"}], pa)
    egress._write_batch([{"key": "b"}], pa)

    assert len(catalog.create_calls) == 1


def test_an_explicit_schema_overrides_inference():
    """Inference reads sim_ts as an integer here; the pinned schema has to win."""
    catalog = FakeCatalog()
    pinned = pa.schema([("key", pa.string()), ("sim_ts", pa.float64())])
    egress = IcebergStorageEgress(
        catalog=catalog, default_table="sim.events", schemas={"sim.events": pinned}
    )

    egress._write_batch([{"key": "a", "sim_ts": 1}], pa)

    identifier, schema, _ = catalog.create_calls[0]
    assert schema == pinned
    assert catalog.tables["sim.events"].appended[0].schema.field("sim_ts").type == (
        pa.float64()
    )


def test_an_explicit_location_is_passed_to_the_catalog():
    """Pinned so a consumer reading by path is not defeated by a random suffix."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.events",
        locations={"sim.events": "s3://warehouse/pinned/events"},
    )

    egress._write_batch([{"key": "a"}], pa)

    assert catalog.create_calls[0][2] == "s3://warehouse/pinned/events"


def test_later_batches_are_cast_to_the_table_schema():
    """A table that already exists governs, not whatever the next batch infers."""
    catalog = FakeCatalog()
    pinned = pa.schema([("key", pa.string()), ("sim_ts", pa.float64())])
    egress = IcebergStorageEgress(
        catalog=catalog, default_table="sim.events", schemas={"sim.events": pinned}
    )

    egress._write_batch([{"key": "a", "sim_ts": 1.5}], pa)
    # sim_ts arrives as an integer, which inference alone would read as int64.
    egress._write_batch([{"key": "b", "sim_ts": 2}], pa)

    second = catalog.tables["sim.events"].appended[1]
    assert second.schema == pinned
    assert second.to_pylist() == [{"key": "b", "sim_ts": 2.0}]
