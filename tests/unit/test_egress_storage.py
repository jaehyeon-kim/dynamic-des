import json
import re
from pathlib import Path

import pytest

# Attempt to import PyArrow, skip tests if not installed
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow import fs

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

from dynamic_des.connectors.egress.storage import (
    JsonlStorageEgress,
    ParquetStorageEgress,
    _generate_chunk_filename,
)

pytestmark = pytest.mark.skipif(
    not HAS_PYARROW, reason="pyarrow is required for storage egress tests"
)


def test_generate_chunk_filename():
    """Test that the UUID chunking suffix is correctly injected before the extension."""
    base_path = "data/simulation_run/events.parquet"
    chunked_path = _generate_chunk_filename(base_path)

    # Use regex to verify it looks like 'data/simulation_run/events_a1b2c3d4.parquet'
    match = re.match(
        r"^data/simulation_run/events_([a-f0-9]{8})\.parquet$", chunked_path
    )

    assert (
        match is not None
    ), f"Filename {chunked_path} did not match expected chunk pattern."
    assert len(match.group(1)) == 8  # Verify it's an 8-character hex


def test_jsonl_storage_routing_and_writing(tmp_path: Path):
    """Test JSONL egress routing, dropping, and file writing."""

    # 1. Define a router that splits events/telemetry and drops lag
    def test_router(data: dict) -> str | None:
        if data.get("path_id") == "lag":
            return None
        if data.get("stream_type") == "telemetry":
            return str(tmp_path / "telemetry.jsonl")
        return str(tmp_path / "events.jsonl")

    # 2. Initialize egress with local file system
    egress = JsonlStorageEgress(path_router=test_router)
    egress.filesystem = fs.LocalFileSystem()

    # 3. Create a dummy batch with mixed data types
    batch = [
        {"stream_type": "event", "val": 1},
        {"stream_type": "telemetry", "val": 2},
        {"stream_type": "telemetry", "path_id": "lag", "val": 3},  # Should be dropped
        {"stream_type": "event", "val": 4},
    ]

    # 4. Execute the batch write
    egress._write_batch(batch)

    # 5. Verify the files were created with UUID chunks
    written_files = list(tmp_path.glob("*.jsonl"))
    assert (
        len(written_files) == 2
    ), "Expected exactly two chunked files (events and telemetry)"

    events_files = list(tmp_path.glob("events_*.jsonl"))
    telemetry_files = list(tmp_path.glob("telemetry_*.jsonl"))

    assert len(events_files) == 1
    assert len(telemetry_files) == 1

    # 6. Verify contents
    with open(events_files[0], "r") as f:
        events_data = [json.loads(line) for line in f]
        assert len(events_data) == 2
        assert events_data[0]["val"] == 1
        assert events_data[1]["val"] == 4

    with open(telemetry_files[0], "r") as f:
        telemetry_data = [json.loads(line) for line in f]
        assert len(telemetry_data) == 1
        assert (
            telemetry_data[0]["val"] == 2
        )  # The lag metric (val 3) was correctly dropped


def test_parquet_storage_schema_drift(tmp_path: Path):
    """Test Parquet egress prevents schema drift between consecutive batches."""

    # Send everything to one file prefix
    base_path = str(tmp_path / "metrics.parquet")

    egress = ParquetStorageEgress(default_path=base_path)
    egress.filesystem = fs.LocalFileSystem()

    # BATCH 1 (Defines the Schema)
    # The 'value' is a FLOAT here
    batch_1 = [{"metric": "temp", "value": 98.6}, {"metric": "speed", "value": 104.2}]
    egress._write_batch(batch_1, pa, pq)

    # BATCH 2 (Introduces Schema Drift)
    # The 'value' is an INTEGER here!
    # Without drift protection, pyarrow would crash or create incompatible files.
    batch_2 = [{"metric": "temp", "value": 99}, {"metric": "speed", "value": 105}]
    egress._write_batch(batch_2, pa, pq)

    # Verification
    written_files = list(tmp_path.glob("metrics_*.parquet"))
    assert len(written_files) == 2, "Expected two separate Parquet chunks."

    # Read the files back into a single PyArrow Dataset
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(tmp_path), format="parquet")
    table = dataset.to_table()

    # 1. Ensure all 4 rows survived
    assert table.num_rows == 4

    # 2. Ensure strict schema typing was enforced!
    # The 'value' column should be purely DOUBLE/FLOAT64, successfully casting the integers from Batch 2.
    value_field = table.schema.field("value")
    assert pa.types.is_float64(
        value_field.type
    ), f"Expected float64, got {value_field.type}"

    # 3. Verify the actual casted data
    val_list = table.column("value").to_pylist()
    assert sorted(val_list) == sorted([98.6, 104.2, 99.0, 105.0])
