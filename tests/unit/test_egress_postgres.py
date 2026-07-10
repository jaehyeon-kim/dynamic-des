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
