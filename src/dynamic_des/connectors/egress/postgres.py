import asyncio
import logging
import queue
from collections.abc import Mapping
from typing import Any

import asyncpg

from dynamic_des.connectors.egress.base import BaseEgress, parse_iso_columns

logger = logging.getLogger(__name__)

# information_schema data types whose columns take a Python datetime or date.
# asyncpg rejects an ISO string for these, so such strings are converted first.
_TIME_TYPES = {
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
    "date": "date",
}


def _table_spec(name: str, spec: Any) -> tuple[dict[str, str], list[str]]:
    """
    Reads one entry of `tables` into its columns and primary key.

    Args:
        name (str): The table name, used in errors.
        spec (Any): A mapping with `columns` (column name to SQL type) and an
            optional `primary_key` (a column name or a list of them).

    Returns:
        tuple[dict[str, str], list[str]]: The columns and the primary key columns.

    Raises:
        ValueError: If the entry has no columns, has an unknown key, or names a
            primary key column it does not define.
    """
    if not isinstance(spec, Mapping) or not isinstance(spec.get("columns"), Mapping):
        raise ValueError(
            f"Table '{name}' needs 'columns', a mapping of column name to SQL type"
        )
    unknown = set(spec) - {"columns", "primary_key"}
    if unknown:
        raise ValueError(
            f"Table '{name}' has unknown key(s) {sorted(unknown)}; "
            "the keys are columns and primary_key"
        )
    columns = {str(column): str(sql) for column, sql in spec["columns"].items()}
    if not columns:
        raise ValueError(f"Table '{name}' needs at least one column")
    key = spec.get("primary_key") or []
    primary_key = [key] if isinstance(key, str) else [str(k) for k in key]
    missing = [k for k in primary_key if k not in columns]
    if missing:
        raise ValueError(
            f"Table '{name}' has primary key column(s) {missing} not in its columns"
        )
    return columns, primary_key


def _create_table_sql(
    name: str, columns: dict[str, str], primary_key: list[str]
) -> str:
    """
    Builds the `CREATE TABLE IF NOT EXISTS` statement for a `tables` entry.

    Names are not quoted, matching the `INSERT` statements this connector writes,
    so both refer to the same table and columns.

    Args:
        name (str): The table name.
        columns (dict[str, str]): Column name to SQL type.
        primary_key (list[str]): The primary key columns, or an empty list.

    Returns:
        str: The statement.
    """
    parts = [f"{column} {sql}" for column, sql in columns.items()]
    if primary_key:
        parts.append(f"PRIMARY KEY ({', '.join(primary_key)})")
    return f"CREATE TABLE IF NOT EXISTS {name} ({', '.join(parts)})"


class PostgresEgress(BaseEgress):
    """
    Asynchronous egress provider for persisting simulation data to PostgreSQL.

    This connector handles long-term storage of simulation results or
    continuous generation of relational CDC data, performing bulk inserts
    of batched data into a specified PostgreSQL table using asyncpg.

    By default a record whose key already exists is skipped (`ON CONFLICT DO
    NOTHING`). Name the key columns in `upsert_keys` to update the existing row
    instead, which a simulation needs when it changes rows it wrote earlier, such
    as an order moving from processing to shipped.

    An ISO time string bound for a timestamp or date column is converted to a
    `datetime` or `date` first, because asyncpg rejects the string itself.

    Tables named in `tables` are created at start when they do not exist, so a run
    needs no separate schema step.

    Examples:
        Updating orders as their status changes:

        ```python
        egress = PostgresEgress(
            "postgresql://user:password@localhost:5432/db",
            table_name="orders",
            upsert_keys=["order_id"],
        )
        ```

        Creating the table at start, in the shape a YAML file gives:

        ```python
        egress = PostgresEgress(
            "postgresql://user:password@localhost:5432/db",
            tables={
                "orders": {
                    "columns": {"order_id": "INT", "status": "TEXT"},
                    "primary_key": ["order_id"],
                }
            },
        )
        ```
    """

    def __init__(
        self,
        connection_dsn: str,
        table_name: str | None = None,
        upsert_keys: list[str] | None = None,
        tables: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        """
        Initializes the PostgresEgress with connection details.

        Args:
            connection_dsn: PostgreSQL connection string (DSN).
            table_name: Target table for simulation records. Defaults to the one
                table in `tables` when it names exactly one, and to
                "simulation_data" when `tables` is not given.
            upsert_keys: Optional key columns. When given, a record whose key
                already exists updates that row's other columns, and the columns
                need a unique index or constraint. When None, such a record is
                skipped.
            tables: Optional tables to create at start when they do not exist. Each
                table name maps to `columns`, a mapping of column name to SQL type,
                and an optional `primary_key`, a column name or a list of them.
            **kwargs: Additional connection pool arguments for asyncpg.

        Raises:
            ValueError: If a `tables` entry is malformed, or if `tables` names
                several tables and `table_name` does not say which one to write to.
        """
        self.tables: dict[str, tuple[dict[str, str], list[str]]] = {
            str(name): _table_spec(str(name), spec)
            for name, spec in (tables or {}).items()
        }
        if table_name is None:
            if len(self.tables) > 1:
                raise ValueError(
                    f"tables names {sorted(self.tables)}; set table_name to the one "
                    "this egress writes to"
                )
            table_name = next(iter(self.tables), "simulation_data")
        self.dsn = connection_dsn
        self.table_name = table_name
        self.upsert_keys = list(upsert_keys) if upsert_keys else None
        self.kwargs = kwargs
        self.pool: asyncpg.Pool | None = None
        self.valid_columns: set[str] = set()
        self.time_columns: dict[str, str] = {}

    async def _init_pool(self) -> None:
        if self.pool is None:
            self.pool = await asyncpg.create_pool(dsn=self.dsn, **self.kwargs)
            logger.info(f"PostgresEgress connected to {self.table_name}")

            # Cache valid columns to safely ignore extra injected fields (like sim_ts)
            self.valid_columns = set()
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                for name, (columns, primary_key) in self.tables.items():
                    try:
                        await conn.execute(
                            _create_table_sql(name, columns, primary_key)
                        )
                    except (
                        asyncpg.exceptions.DuplicateTableError,
                        asyncpg.exceptions.UniqueViolationError,
                    ):
                        # Another writer created it between the existence check
                        # and the create, which IF NOT EXISTS does not guard.
                        pass
                rows = await conn.fetch(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = $1",
                    self.table_name,
                )
                self.valid_columns = {row["column_name"] for row in rows}
                self.time_columns = {
                    row["column_name"]: _TIME_TYPES[row.get("data_type")]
                    for row in rows
                    if row.get("data_type") in _TIME_TYPES
                }
                if not self.valid_columns:
                    logger.warning(
                        f"Table '{self.table_name}' does not exist or has no columns!"
                    )
                elif self.upsert_keys:
                    missing = set(self.upsert_keys) - self.valid_columns
                    if missing:
                        raise ValueError(
                            f"Table '{self.table_name}' has no column(s) "
                            f"{sorted(missing)} to use as upsert keys"
                        )

    def _column_runs(self, records: list[dict]) -> list[tuple[list[str], list[dict]]]:
        """
        Splits a batch into runs of consecutive records that carry the same columns.

        Only columns the table has are kept. A record with none of them is dropped.

        Args:
            records: The batch's records, in the order they arrived.

        Returns:
            list[tuple[list[str], list[dict]]]: Each run's columns and its records.
        """
        runs: list[tuple[list[str], list[dict]]] = []
        for record in records:
            keys = [k for k in record if k in self.valid_columns]
            if not keys:
                continue
            if runs and runs[-1][0] == keys:
                runs[-1][1].append(record)
            else:
                runs.append((keys, [record]))
        return runs

    def _insert_query(self, keys: list[str]) -> str:
        """
        Builds the insert statement for a batch's columns.

        Without `upsert_keys` a conflicting record is skipped, which keeps a
        restarted simulation from failing on rows it already wrote. With
        `upsert_keys` it updates the columns the record carries, apart from the
        keys themselves.

        Args:
            keys: The batch's columns, in the order of the values.

        Returns:
            str: The `INSERT` statement, with one `$n` placeholder per column.

        Raises:
            ValueError: If a key column in `upsert_keys` is missing from the batch.
        """
        columns = ", ".join(keys)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(keys)))
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        if not self.upsert_keys:
            return f"{query} ON CONFLICT DO NOTHING"
        missing = [k for k in self.upsert_keys if k not in keys]
        if missing:
            raise ValueError(
                f"Records for {self.table_name} lack the upsert key column(s) {missing}"
            )
        conflict = ", ".join(self.upsert_keys)
        updates = [k for k in keys if k not in self.upsert_keys]
        if not updates:
            return f"{query} ON CONFLICT ({conflict}) DO NOTHING"
        assignments = ", ".join(f"{k} = EXCLUDED.{k}" for k in updates)
        return f"{query} ON CONFLICT ({conflict}) DO UPDATE SET {assignments}"

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        Main execution loop for PostgreSQL data persistence.

        Args:
            egress_queue: A thread-safe queue containing batches of simulation data.

        Raises:
            ValueError: If a column in `upsert_keys` is not in the table.
        """
        await self._init_pool()

        while True:
            try:
                # Receive a batch (list) of dictionaries
                batch = egress_queue.get_nowait()
                if not batch:
                    continue

                # Support multi-table multiplexing using a __table__ key
                clean_batch = []
                for item in batch:
                    # `item` is an EventPayload or TelemetryPayload dict.
                    # The actual user data is in `item["value"]`.
                    payload = item.get("value")

                    if not isinstance(payload, dict):
                        # PostgresEgress requires dictionaries (or Pydantic models dumped to dicts)
                        continue

                    # Merge the simulation metadata into the payload so it CAN be inserted if the user configured their schema to accept it
                    payload_copy = payload.copy()
                    payload_copy["sim_ts"] = item.get("sim_ts")
                    payload_copy["timestamp"] = item.get("timestamp")

                    if "key" in item:
                        payload_copy["event_id"] = item["key"]
                    if "path_id" in item:
                        payload_copy["path_id"] = item["path_id"]

                    # Determine target table, defaulting to self.table_name if not specified
                    target = payload_copy.get("__table__", self.table_name)
                    if target == self.table_name:
                        payload_copy.pop("__table__", None)
                        clean_batch.append(payload_copy)

                if not clean_batch:
                    continue

                # The records carry times as ISO strings, which asyncpg rejects for
                # a timestamp or date column.
                clean_batch = parse_iso_columns(clean_batch, self.time_columns)

                # Each run of records with the same columns is one statement, in the
                # order they arrived. A statement built from one record's columns
                # would set another record's missing columns to NULL on an upsert.
                runs = self._column_runs(clean_batch)
                if not runs:
                    logger.warning(
                        f"No matching columns for table {self.table_name}. Skipping."
                    )
                    continue

                assert self.pool is not None
                async with self.pool.acquire() as conn:
                    for keys, records in runs:
                        values = [tuple(data.get(k) for k in keys) for data in records]
                        await conn.executemany(self._insert_query(keys), values)

                written = sum(len(records) for _, records in runs)
                logger.debug(f"Inserted {written} records into {self.table_name}")

            except queue.Empty:
                # Yield to the event loop if the queue is empty
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error inserting batch into {self.table_name}: {e}")
                # Back-off on database failures to prevent rapid crash looping
                await asyncio.sleep(1)
