import asyncio
import queue
import pytest
import asyncpg

from dynamic_des.connectors.egress.postgres import PostgresEgress
from dynamic_des.connectors.ingress.postgres import PostgresIngress


@pytest.mark.asyncio
@pytest.mark.integration
async def test_postgres_ingress_loads_historical_state(postgres_container):
    """
    Tests that PostgresIngress correctly executes its Phase 1 startup
    by loading the latest is_applied = TRUE row from history.
    """
    dsn = postgres_container
    
    # 1. Manually set up the database and history
    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP TABLE IF EXISTS test_sim_params")
    await conn.execute("""
        CREATE TABLE test_sim_params (
            id SERIAL PRIMARY KEY,
            param_path TEXT,
            param_value TEXT,
            is_applied BOOLEAN DEFAULT FALSE
        );
    """)
    
    # Insert multiple historical records for the same path
    await conn.execute("""
        INSERT INTO test_sim_params (param_path, param_value, is_applied)
        VALUES 
            ('Test.param', '1.0', TRUE),
            ('Test.param', '5.0', TRUE),
            ('Test.param', '10.0', FALSE);
    """)
    await conn.close()

    ingress = PostgresIngress(dsn, table_name="test_sim_params")
    q = queue.Queue()
    
    # 2. Run the ingress loop in a background task
    task = asyncio.create_task(ingress.run(q))
    
    # 3. Wait a brief moment for it to process Phase 1 and Phase 2
    await asyncio.sleep(1.0)
    task.cancel()
    
    items = []
    while not q.empty():
        items.append(q.get_nowait())
        
    assert len(items) == 2
    assert items[0] == ("Test.param", 5.0)   # Phase 1: Historical load parsed as float
    assert items[1] == ("Test.param", 10.0)  # Phase 2: New update fetch parsed as float
    
    # Verify it updated the table marking ID 3 as applied
    conn = await asyncpg.connect(dsn)
    is_applied = await conn.fetchval("SELECT is_applied FROM test_sim_params WHERE param_value = '10.0'")
    assert is_applied is True
    await conn.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_postgres_egress_column_filtering(postgres_container):
    """
    Tests that PostgresEgress successfully filters out extraneous dict keys
    and dynamically inserts into a table.
    """
    dsn = postgres_container
    
    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP TABLE IF EXISTS test_orders")
    await conn.execute("""
        CREATE TABLE test_orders (
            order_id INT PRIMARY KEY,
            total_amount REAL
        );
    """)
    await conn.close()
    
    egress = PostgresEgress(dsn, table_name="test_orders")
    q = queue.Queue()
    
    mock_event = [{
        "key": "order-1",
        "sim_ts": 123.4,
        "timestamp": "2026-07-10T12:00:00Z",
        "value": {
            "__table__": "test_orders",
            "order_id": 99,
            "total_amount": 100.50,
            "extra_field": "should_be_ignored"
        }
    }]
    
    q.put(mock_event)
    
    # Run the egress to process the batch
    task = asyncio.create_task(egress.run(q))
    await asyncio.sleep(1.0)
    task.cancel()
    
    # Verify the insertion
    conn = await asyncpg.connect(dsn)
    rows = await conn.fetch("SELECT * FROM test_orders")
    await conn.close()
    
    assert len(rows) == 1
    assert rows[0]['order_id'] == 99
    assert rows[0]['total_amount'] == 100.50
