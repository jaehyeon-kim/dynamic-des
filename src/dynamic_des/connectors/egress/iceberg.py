import asyncio
import logging
import queue
from collections.abc import Mapping
from typing import Any, Callable, Dict, Optional

from dynamic_des.connectors.egress.base import (
    BaseEgress,
    group_rows,
    parse_iso_columns,
)

logger = logging.getLogger(__name__)


# Iceberg's primitive type names, as accepted in a `schemas` mapping, and the
# PyArrow type each one is created with.
_TYPE_NAMES = {
    "string": ("string", ()),
    "int": ("int32", ()),
    "long": ("int64", ()),
    "float": ("float32", ()),
    "double": ("float64", ()),
    "boolean": ("bool_", ()),
    "timestamp": ("timestamp", ("us",)),
    "timestamptz": ("timestamp", ("us", "UTC")),
    "date": ("date32", ()),
    "binary": ("binary", ()),
}


def _arrow_schema(identifier: str, schema: Any) -> Any:
    """
    Returns a PyArrow schema for a `schemas` entry.

    Args:
        identifier (str): The table the entry is for, named in errors.
        schema (Any): A PyArrow schema, which is returned as it is, or a mapping of
            column name to an Iceberg type name such as `string` or `timestamp`.

    Returns:
        Any: The PyArrow schema.

    Raises:
        ValueError: If a type name is not one of the supported names.
    """
    if not isinstance(schema, Mapping):
        return schema
    import pyarrow as pa

    fields = []
    for column, type_name in schema.items():
        spec = _TYPE_NAMES.get(str(type_name).lower())
        if spec is None:
            raise ValueError(
                f"Column '{column}' of {identifier} has the type '{type_name}'; "
                f"the supported types are {', '.join(_TYPE_NAMES)}"
            )
        factory, args = spec
        fields.append((column, getattr(pa, factory)(*args)))
    return pa.schema(fields)


def _catalog_property(value: Any) -> Any:
    """Renders a YAML scalar as the string PyIceberg reads catalog properties as."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None or isinstance(value, (str, Mapping)):
        return value
    return str(value)


def _time_columns(schema: Any, pa: Any) -> Dict[str, str]:
    """
    Names the timestamp and date columns of an Arrow schema.

    Args:
        schema (Any): The table's PyArrow schema.
        pa (Any): Injected reference to the `pyarrow` module.

    Returns:
        Dict[str, str]: Column name to `timestamp`, `timestamptz` or `date`.
    """
    columns: Dict[str, str] = {}
    for field in schema:
        if pa.types.is_timestamp(field.type):
            columns[field.name] = "timestamptz" if field.type.tz else "timestamp"
        elif pa.types.is_date(field.type):
            columns[field.name] = "date"
    return columns


class IcebergStorageEgress(BaseEgress):
    """
    Apache Iceberg table writer for lakehouse ingestion patterns.

    `ParquetStorageEgress` writes files and leaves registration to the caller. This
    connector appends straight into an Iceberg table instead, so one run can feed a
    stream sink and a queryable table in the same pass. Each flush of a provider's
    buffer becomes exactly one Iceberg commit, which is why the buffer size matters:
    every commit writes a manifest, a manifest list and a new `metadata.json`, and
    query planning degrades as the snapshot count grows. Give this provider a large
    `batch_size` on `add_egress` so the run produces a handful of snapshots rather
    than hundreds.

    Without a `table_router`, events are written to `default_table` as flat rows:
    each event's `value` mapping becomes columns, and telemetry is left out. With a
    router, records are written as the router leaves them.

    By default every record is appended. Name a table's key columns in
    `upsert_keys` to write it with PyIceberg's `upsert` instead: a record whose key
    exists updates that row, and a new key is inserted, so rerunning a seeded
    simulation over the same window does not duplicate rows. An upsert reads the
    matching rows before writing, so it is slower than an append, and it never
    deletes rows. It replaces the whole row, so every record must carry every column: a
    missing column is written as null. Within one flush, the last record for a key wins. An upsert
    flush is still one commit, but it can add up to three snapshots: an overwrite
    and two appends.

    The catalog is supplied by the caller, either as a built `Catalog` or as a
    mapping of PyIceberg catalog properties that is passed to `load_catalog`. The
    supported catalog is Iceberg REST, which is what the integration tests run
    against.

    Schemas are inferred from the first batch per table and reused for later ones,
    exactly as the Parquet writer does. Inference reads an ISO timestamp as a string,
    so a table whose consumer expects a real timestamp needs an explicit schema. An
    ISO string bound for a timestamp or date column of the table is converted to a
    `datetime` or `date` before the write, because PyArrow rejects the string itself.

    Attributes:
        catalog (Any): An instantiated pyiceberg `Catalog`, or None until it is built
            from `catalog_properties`.
        catalog_properties (Optional[Dict[str, Any]]): The PyIceberg catalog
            properties given instead of a `Catalog`, or None.
        default_table (Optional[str]): Fallback `namespace.table` when no router is given.
        table_router (Optional[Callable]): Logic returning `namespace.table`, or None to
            drop the record.
        schemas (Dict[str, Any]): PyArrow schema per table identifier. Seeded with any
            explicit schemas, then filled in from the tables that get created.
        locations (Dict[str, str]): Explicit storage location per table identifier.
        upsert_keys (Dict[str, list]): Key columns per table identifier, for the
            tables written with `upsert` rather than `append`.

    Examples:
        Splitting events and telemetry into two tables on a REST catalog:

        ```python
        import pyarrow as pa
        from pyiceberg.catalog.rest import RestCatalog

        catalog = RestCatalog(
            "odctl", uri="http://localhost:8181", warehouse="s3://warehouse/"
        )

        def table_router(data: dict) -> str | None:
            if data.get("path_id") == "system.simulation.lag_seconds":
                return None  # drop
            if data.get("stream_type") == "telemetry":
                return "simulation.telemetry"
            return "simulation.events"

        egress = IcebergStorageEgress(
            catalog=catalog,
            table_router=table_router,
            schemas={
                "simulation.events": pa.schema(
                    [
                        ("stream_type", pa.string()),
                        ("sim_ts", pa.float64()),
                        ("timestamp", pa.timestamp("us")),
                        ("key", pa.string()),
                    ]
                )
            },
        )

        app.add_egress(egress, batch_size=200_000)
        ```
    """

    def __init__(
        self,
        catalog: Any,
        default_table: Optional[str] = None,
        table_router: Optional[Callable[[dict], Optional[str]]] = None,
        schemas: Optional[Dict[str, Any]] = None,
        locations: Optional[Dict[str, str]] = None,
        upsert_keys: Optional[Dict[str, list]] = None,
    ):
        """
        Initializes the IcebergStorageEgress with a catalog and routing settings.

        Args:
            catalog: An instantiated pyiceberg `Catalog`, already configured with its
                URI, warehouse and credentials. A mapping of PyIceberg catalog
                properties (`uri`, `warehouse`, `s3.endpoint` and so on) is also
                accepted: the catalog is then built with PyIceberg's `load_catalog`
                on the first write, with an optional `name` key as the catalog name.
            default_table: The target `namespace.table` used when no router is given.
                Only events are written there, with their `value` mapping unpacked
                into columns.
            table_router: A function taking a dict payload and returning a
                `namespace.table` identifier, or None to drop the record.
            schemas: Optional PyArrow schema per table identifier. A table named here
                is created with that schema and every batch is cast to it, instead of
                the schema being inferred from the first batch. A schema can also be
                a mapping of column name to an Iceberg type name: string, int, long,
                float, double, boolean, timestamp, timestamptz, date or binary.
            locations: Optional storage location per table identifier. Pin this when a
                consumer reads the table by path, because a catalog with
                `unique-table-location` set otherwise appends a random suffix.
            upsert_keys: Optional key columns per table identifier. A table named
                here is written with `upsert` on those columns; any other table is
                appended to.

        Raises:
            ValueError: If neither `default_table` nor `table_router` is given, since
                the connector would then have nowhere to write.
            ImportError: If `upsert_keys` is given and the installed pyiceberg is
                older than 0.9.0, which added `Table.upsert`.
        """
        if default_table is None and table_router is None:
            raise ValueError(
                "IcebergStorageEgress needs default_table or table_router; "
                "without one it has no table to write to"
            )
        # A mapping of properties is loaded on the first write rather than here,
        # because a REST catalog contacts its server as soon as it is built.
        self.catalog_properties: Optional[Dict[str, Any]] = None
        self.catalog = catalog
        if isinstance(catalog, Mapping):
            self.catalog_properties = dict(catalog)
            self.catalog = None
        self.default_table = default_table
        self.table_router = table_router
        self.schemas: Dict[str, Any] = {
            identifier: _arrow_schema(identifier, schema)
            for identifier, schema in (schemas or {}).items()
        }
        self.locations: Dict[str, str] = dict(locations or {})
        self.upsert_keys: Dict[str, list] = {
            identifier: list(keys) for identifier, keys in (upsert_keys or {}).items()
        }
        if self.upsert_keys:
            from pyiceberg.table import Table

            # Table.upsert arrived in pyiceberg 0.9.0; the package floor is lower
            # because appending works on older releases.
            if not hasattr(Table, "upsert"):
                raise ImportError(
                    "upsert_keys needs pyiceberg 0.9.0 or later: "
                    'pip install "pyiceberg>=0.9.0"'
                )
        # Loaded tables, kept so a batch costs one append rather than a catalog
        # round trip as well. pyiceberg retries the commit if the handle is stale,
        # so another writer changing the table does not lose this one's records.
        self._tables: Dict[str, Any] = {}

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        The main execution loop that consumes the egress queue and commits to Iceberg.

        This loop continuously polls the internal `egress_queue` for data batches.
        When a batch is received, it offloads the synchronous PyArrow conversion and
        the Iceberg commit to a background thread, so the asyncio loop is not blocked
        while the catalog is written.

        Args:
            egress_queue: A thread-safe queue containing batches of dictionaries
                generated by the environment's EgressMixIn.

        Raises:
            ImportError: If the 'pyarrow' package is not installed.

        Note:
            The loop exits gracefully upon receiving an `asyncio.CancelledError`, so
            the final commit has completed before shutdown returns.
        """
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError(
                "pyarrow is required to build the batches. "
                "pip install dynamic-des[iceberg]"
            )

        batches_processed = 0

        try:
            while True:
                try:
                    batch = egress_queue.get_nowait()
                    self.active_tasks = getattr(self, "active_tasks", 0) + 1
                    try:
                        await asyncio.to_thread(self._write_batch, batch, pa)
                    finally:
                        self.active_tasks -= 1
                    # Log a progress heartbeat every 10 batches
                    batches_processed += 1
                    if batches_processed % 10 == 0:
                        q_size = egress_queue.qsize()
                        logger.info(
                            f"Iceberg Writer: Committed {batches_processed} batches. "
                            f"(~{q_size} batches waiting in queue)"
                        )
                except queue.Empty:
                    await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            # This triggers during env.teardown()
            logger.info(
                f"IcebergStorageEgress shut down requested. "
                f"Successfully committed {batches_processed} total snapshots."
            )

    def _write_batch(self, batch: list, pa: Any):
        """
        Groups a batch by target table and writes each group in one commit.

        One write per table per batch is what keeps the commit count equal to the
        flush count. Splitting a batch into several writes would multiply the
        metadata a reader has to walk through. A table in `upsert_keys` is upserted,
        after keeping only the last record per key, because PyIceberg rejects a
        batch with duplicate keys.

        Args:
            batch (list): A list of dictionaries or Pydantic models to be written.
            pa (Any): Injected reference to the `pyarrow` module.

        Raises:
            ValueError: If a record for an upserted table lacks a key column.
        """
        grouped_batches = group_rows(batch, self.table_router, self.default_table)

        for identifier, records in grouped_batches.items():
            table = self._resolve_table(identifier, records, pa)
            records = parse_iso_columns(
                records, _time_columns(self.schemas[identifier], pa)
            )
            keys = self.upsert_keys.get(identifier)
            if keys:
                missing = sorted({k for r in records for k in keys if r.get(k) is None})
                if missing:
                    # A null key never matches, so a rerun would insert the record again.
                    raise ValueError(
                        f"Records for {identifier} lack the upsert key column(s) {missing}"
                    )
                latest = {tuple(r[k] for k in keys): r for r in records}
                df = pa.Table.from_pylist(
                    list(latest.values()), schema=self.schemas[identifier]
                )
                table.upsert(df, join_cols=keys)
            else:
                table.append(
                    pa.Table.from_pylist(records, schema=self.schemas[identifier])
                )

    def _resolve_table(self, identifier: str, records: list, pa: Any) -> Any:
        """
        Returns the loaded table for an identifier, creating it on first use.

        The schema comes from `schemas` when the caller pinned one, and is inferred
        from this first batch otherwise. Either way the created table's own schema
        becomes the cast target for every later batch, so a table that already
        existed governs rather than the records that happen to arrive first.

        Args:
            identifier (str): A `namespace.table` identifier.
            records (list): The first batch destined for this table.
            pa (Any): Injected reference to the `pyarrow` module.

        Returns:
            Any: The pyiceberg `Table` to append to.

        Raises:
            ValueError: If the identifier names no namespace.
        """
        table = self._tables.get(identifier)
        if table is not None:
            return table

        if "." not in identifier:
            raise ValueError(
                f"'{identifier}' names no namespace; Iceberg identifiers are "
                "'namespace.table'"
            )
        namespace = identifier.rsplit(".", 1)[0]

        schema = self.schemas.get(identifier) or pa.Table.from_pylist(records).schema
        catalog = self._load_catalog()
        catalog.create_namespace_if_not_exists(namespace)
        table = catalog.create_table_if_not_exists(
            identifier, schema=schema, location=self.locations.get(identifier)
        )

        # Read the cast target back from the table rather than trusting what was sent,
        # because a table that already existed keeps the schema it was created with.
        self.schemas[identifier] = table.schema().as_arrow()
        self._tables[identifier] = table
        return table

    def _load_catalog(self) -> Any:
        """
        Returns the catalog, building it from `catalog_properties` on first use.

        Returns:
            Any: The pyiceberg `Catalog`.
        """
        if self.catalog is None:
            from pyiceberg.catalog import load_catalog

            properties = {
                key: _catalog_property(value)
                for key, value in (self.catalog_properties or {}).items()
            }
            name = properties.pop("name", None)
            self.catalog = load_catalog(name, **properties)
        return self.catalog
