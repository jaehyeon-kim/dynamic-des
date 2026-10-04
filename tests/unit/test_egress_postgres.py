import asyncio
import queue
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from dynamic_des.connectors.egress.postgres import PostgresEgress


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__.return_value = conn
    pool.acquire.return_value = acquire_ctx
    return pool, conn


@pytest.mark.asyncio
async def test_postgres_egress_initialization():
    egress = PostgresEgress("postgresql://user:password@localhost/db", "test_table")
    assert egress.dsn == "postgresql://user:password@localhost/db"
    assert egress.table_name == "test_table"


@pytest.mark.asyncio
@patch(
    "dynamic_des.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock
)
async def test_postgres_egress_run(mock_create_pool, mock_pool):
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": "id"}, {"column_name": "value"}]
    egress = PostgresEgress("postgresql://user:password@localhost/db", "test_table")
    egress_queue = queue.Queue()
    batch = [
        {"value": {"__table__": "test_table", "id": 1, "value": "a"}},
        {"value": {"__table__": "other_table", "id": 2, "value": "b"}},
        {"value": {"id": 3, "value": "c"}},
    ]
    egress_queue.put(batch)
    task = asyncio.create_task(egress.run(egress_queue))
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    mock_create_pool.assert_called_once()
    assert conn.executemany.call_count == 1
    call_args = conn.executemany.call_args[0]
    query, values = call_args[0], call_args[1]
    assert "INSERT INTO test_table" in query
    assert "ON CONFLICT DO NOTHING" in query
    assert len(values) == 2
    assert (1, "a") in values
    assert (3, "c") in values


def test_default_query_skips_conflicts():
    """Without upsert_keys a record whose key exists is skipped, as before."""
    egress = PostgresEgress("postgresql://u:p@localhost/db", "orders")
    assert egress._insert_query(["order_id", "status"]) == (
        "INSERT INTO orders (order_id, status) VALUES ($1, $2) ON CONFLICT DO NOTHING"
    )


def test_upsert_query_updates_the_other_columns():
    """With upsert_keys a conflict on the key updates every other column the batch carries."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "orders", upsert_keys=["order_id"]
    )
    assert egress._insert_query(["order_id", "status", "shipped_at"]) == (
        "INSERT INTO orders (order_id, status, shipped_at) VALUES ($1, $2, $3) "
        "ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status, "
        "shipped_at = EXCLUDED.shipped_at"
    )


def test_upsert_with_only_key_columns_does_nothing_on_conflict():
    """A batch made only of key columns has nothing to update."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "links", upsert_keys=["a", "b"]
    )
    assert egress._insert_query(["a", "b"]).endswith("ON CONFLICT (a, b) DO NOTHING")


def test_upsert_batch_without_the_key_raises():
    """A batch missing a key column cannot be upserted, and says which one."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "orders", upsert_keys=["order_id"]
    )
    with pytest.raises(ValueError, match="order_id"):
        egress._insert_query(["status"])


@pytest.mark.asyncio
@patch(
    "dynamic_des.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock
)
async def test_upsert_key_missing_from_the_table_fails_at_start(
    mock_create_pool, mock_pool
):
    """An upsert key the table does not have stops the writer before any batch."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": "id"}, {"column_name": "status"}]
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "orders", upsert_keys=["order_id"]
    )
    with pytest.raises(ValueError, match="order_id"):
        await egress.run(queue.Queue())


def test_records_with_different_columns_are_written_in_separate_runs():
    """A partial record gets its own statement, so it cannot set another record's columns to NULL."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "orders", upsert_keys=["order_id"]
    )
    egress.valid_columns = {"order_id", "status", "total"}
    full_1 = {"order_id": 1, "status": "new", "total": 10.0}
    partial = {"order_id": 1, "status": "shipped"}
    full_2 = {"order_id": 2, "status": "new", "total": 5.0, "sim_ts": 1.0}

    runs = egress._column_runs([full_1, partial, full_2])

    assert [keys for keys, _ in runs] == [
        ["order_id", "status", "total"],
        ["order_id", "status"],
        ["order_id", "status", "total"],
    ]
    assert [len(records) for _, records in runs] == [1, 1, 1]


@pytest.mark.asyncio
@patch(
    "dynamic_des.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock
)
async def test_iso_strings_are_converted_for_time_columns(mock_create_pool, mock_pool):
    """asyncpg rejects a string for a timestamp column, so the writer converts it."""
    from datetime import date, datetime, timezone

    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [
        {"column_name": "order_id", "data_type": "integer"},
        {"column_name": "timestamp", "data_type": "timestamp without time zone"},
        {"column_name": "placed_at", "data_type": "timestamp with time zone"},
        {"column_name": "order_day", "data_type": "date"},
        {"column_name": "note", "data_type": "text"},
    ]
    egress = PostgresEgress("postgresql://u:p@localhost/db", "orders")
    egress_queue = queue.Queue()
    egress_queue.put(
        [
            {
                "timestamp": "2026-01-01T12:00:00.000",
                "value": {
                    "order_id": 1,
                    "placed_at": "2026-01-01T12:00:00+00:00",
                    "order_day": "2026-01-01",
                    "note": "2026-01-01",
                },
            }
        ]
    )
    task = asyncio.create_task(egress.run(egress_queue))
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    query, values = conn.executemany.call_args[0]
    row = dict(zip(query.split("(")[1].split(")")[0].split(", "), values[0]))
    assert row["timestamp"] == datetime(2026, 1, 1, 12, 0)
    assert row["placed_at"] == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert row["order_day"] == date(2026, 1, 1)
    assert row["note"] == "2026-01-01"


ORDERS = {
    "columns": {"order_id": "INT", "status": "TEXT", "placed_at": "TIMESTAMP"},
    "primary_key": ["order_id"],
}


@pytest.mark.asyncio
@patch(
    "dynamic_des.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock
)
async def test_tables_are_created_before_the_columns_are_read(
    mock_create_pool, mock_pool
):
    """Each table in tables is created at start, and then its columns are read."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    order = []
    conn.execute.side_effect = lambda sql: order.append(("execute", sql))
    conn.fetch.side_effect = lambda *a: (
        order.append(("fetch",))
        or [{"column_name": "order_id", "data_type": "integer"}]
    )
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db",
        tables={"orders": ORDERS, "items": {"columns": {"item_id": "INT"}}},
        table_name="orders",
    )

    await egress._init_pool()

    assert order == [
        (
            "execute",
            "CREATE TABLE IF NOT EXISTS orders (order_id INT, status TEXT, "
            "placed_at TIMESTAMP, PRIMARY KEY (order_id))",
        ),
        ("execute", "CREATE TABLE IF NOT EXISTS items (item_id INT)"),
        ("fetch",),
    ]
    assert egress.valid_columns == {"order_id"}


@pytest.mark.asyncio
@patch(
    "dynamic_des.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock
)
async def test_a_table_created_by_another_writer_meanwhile_is_ignored(
    mock_create_pool, mock_pool
):
    """Two writers creating one table at once must not stop either of them."""
    import asyncpg

    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.execute.side_effect = asyncpg.exceptions.UniqueViolationError("pg_type")
    conn.fetch.return_value = [{"column_name": "order_id", "data_type": "integer"}]
    egress = PostgresEgress("postgresql://u:p@localhost/db", tables={"orders": ORDERS})

    await egress._init_pool()

    assert egress.valid_columns == {"order_id"}


def test_table_name_defaults_from_tables():
    """One table in tables is the target; none keeps the old default."""
    one = PostgresEgress("postgresql://u:p@localhost/db", tables={"orders": ORDERS})
    none = PostgresEgress("postgresql://u:p@localhost/db")

    assert one.table_name == "orders"
    assert none.table_name == "simulation_data"


def test_several_tables_need_a_table_name():
    """With several tables the writer cannot guess which one it fills."""
    with pytest.raises(ValueError, match="table_name"):
        PostgresEgress(
            "postgresql://u:p@localhost/db",
            tables={"orders": ORDERS, "items": {"columns": {"item_id": "INT"}}},
        )


def test_primary_key_may_be_one_name():
    """A single key column can be written without a list."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db",
        tables={"orders": {"columns": {"order_id": "INT"}, "primary_key": "order_id"}},
    )
    assert egress.tables["orders"] == ({"order_id": "INT"}, ["order_id"])


@pytest.mark.parametrize(
    "spec, message",
    [
        ({"order_id": "INT"}, "needs 'columns'"),
        ({"columns": {}}, "at least one column"),
        ({"columns": {"a": "INT"}, "primary_key": ["b"]}, r"\['b'\]"),
        ({"columns": {"a": "INT"}, "indexes": []}, "unknown key"),
    ],
)
def test_malformed_tables_are_refused_at_construction(spec, message):
    """A mistake in tables fails when the egress is built, not at the first batch."""
    with pytest.raises(ValueError, match=message):
        PostgresEgress("postgresql://u:p@localhost/db", tables={"orders": spec})
