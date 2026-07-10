import asyncio
import queue
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynamic_des.connectors.egress.postgres import PostgresEgress
from dynamic_des.connectors.ingress.postgres import PostgresIngress


@pytest.fixture
def mock_pool():
    """Fixture to mock asyncpg.create_pool and its connections."""
    pool = MagicMock()
    conn = AsyncMock()
    
    # Mock the acquire context manager: async with pool.acquire() as conn:
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
@patch("dynamic_des.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_postgres_egress_run(mock_create_pool, mock_pool):
    pool, conn = mock_pool
    mock_create_pool.return_value = pool

    egress = PostgresEgress("postgresql://user:password@localhost/db", "test_table")
    egress_queue = queue.Queue()

    # Create a batch with mixed table targets
    batch = [
        {"__table__": "test_table", "id": 1, "value": "a", "stream_type": "event"},
        {"__table__": "other_table", "id": 2, "value": "b"}, # Should be skipped
        {"id": 3, "value": "c"}  # Should default to self.table_name
    ]
    egress_queue.put(batch)

    # Run the egress loop briefly and then cancel it
    task = asyncio.create_task(egress.run(egress_queue))
    await asyncio.sleep(0.1)
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Assert connection was made
    mock_create_pool.assert_called_once()
    
    # Assert executemany was called exactly once with the filtered records
    assert conn.executemany.call_count == 1
    call_args = conn.executemany.call_args[0]
    
    query = call_args[0]
    values = call_args[1]
    
    assert "INSERT INTO test_table" in query
    assert "ON CONFLICT DO NOTHING" in query
    
    # id=2 should be missing because it was for "other_table"
    assert len(values) == 2
    assert (1, "a") in values
    assert (3, "c") in values


@pytest.mark.asyncio
@patch("dynamic_des.connectors.ingress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_postgres_ingress_run(mock_create_pool, mock_pool):
    pool, conn = mock_pool
    mock_create_pool.return_value = pool

    # Mock the return value of the fetch query for parameters
    # The ingress should automatically parse json values
    conn.fetch.side_effect = [
        [
            {"id": 1, "param_path": "sim.rate", "param_value": "5.5"},
            {"id": 2, "param_path": "sim.enabled", "param_value": "true"},
            {"id": 3, "param_path": "sim.string", "param_value": '"hello"'}
        ],
        []
    ]

    ingress = PostgresIngress("postgresql://user:password@localhost/db", "params", poll_interval=0.1)
    ingress_queue = queue.Queue()

    # Run the ingress loop briefly
    task = asyncio.create_task(ingress.run(ingress_queue))
    await asyncio.sleep(0.15)
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Assert connection and table init
    mock_create_pool.assert_called_once()
    assert conn.execute.call_count >= 2 # Once for CREATE TABLE, once for UPDATE
    
    # Assert updates were sent to the queue correctly typed
    assert ingress_queue.qsize() == 3
    assert ingress_queue.get_nowait() == ("sim.rate", 5.5)
    assert ingress_queue.get_nowait() == ("sim.enabled", True)
    assert ingress_queue.get_nowait() == ("sim.string", "hello")
