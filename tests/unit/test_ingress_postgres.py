import asyncio
import queue
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from dynamic_des.connectors.ingress.postgres import PostgresIngress


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__.return_value = conn
    pool.acquire.return_value = acquire_ctx
    return pool, conn


@pytest.mark.asyncio
@patch(
    "dynamic_des.connectors.ingress.postgres.asyncpg.create_pool",
    new_callable=AsyncMock,
)
async def test_postgres_ingress_run(mock_create_pool, mock_pool):
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.side_effect = [
        [
            {"id": 1, "param_path": "sim.rate", "param_value": "5.5"},
            {"id": 2, "param_path": "sim.enabled", "param_value": "true"},
            {"id": 3, "param_path": "sim.string", "param_value": '"hello"'},
        ],
        [],
    ]
    ingress = PostgresIngress(
        "postgresql://user:password@localhost/db", "params", poll_interval=0.1
    )
    ingress_queue = queue.Queue()
    task = asyncio.create_task(ingress.run(ingress_queue))
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    mock_create_pool.assert_called_once()
    assert conn.execute.call_count >= 1
    assert ingress_queue.qsize() == 3
    assert ingress_queue.get_nowait() == ("sim.rate", 5.5)
    assert ingress_queue.get_nowait() == ("sim.enabled", True)
    assert ingress_queue.get_nowait() == ("sim.string", "hello")
