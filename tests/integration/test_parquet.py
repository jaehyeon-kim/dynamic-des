import asyncio
import queue
import pytest
from pathlib import Path
import pyarrow.parquet as pq

from dynamic_des.connectors.egress.storage import ParquetStorageEgress


@pytest.mark.asyncio
@pytest.mark.integration
async def test_parquet_egress_local_file(tmp_path: Path):
    """
    Tests that the ParquetStorageEgress successfully buffers and writes
    a pyarrow table to a local Parquet chunk file.
    """
    target_path = str(tmp_path / "test_events.parquet")

    # Use default local filesystem inside ParquetStorageEgress
    egress = ParquetStorageEgress(default_path=target_path)
    q: queue.Queue[list[dict]] = queue.Queue()

    mock_batch = [
        {"key": "order-1", "value": {"order_id": 1, "amount": 100.0}},
        {"key": "order-2", "value": {"order_id": 2, "amount": 250.5}},
    ]

    q.put(mock_batch)

    # Run the egress to process the batch
    task = asyncio.create_task(egress.run(q))
    await asyncio.sleep(1.0)
    task.cancel()

    # Find the uniquely generated chunk file
    parquet_files = list(tmp_path.glob("test_events_*.parquet"))
    assert len(parquet_files) == 1, "Parquet file was not created"

    # Read and verify the contents using PyArrow
    table = pq.read_table(parquet_files[0])
    records = table.to_pylist()

    assert len(records) == 2
    assert "amount" in records[0]["value"]
    assert records[0]["value"]["amount"] == 100.0
    assert records[1]["value"]["order_id"] == 2
