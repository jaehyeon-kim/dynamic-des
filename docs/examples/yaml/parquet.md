# Fast-Forward to Parquet (YAML)

This example builds the same simulation as the [declarative Parquet example](../declarative/parquet.md) from a YAML blueprint. With `factor: 0.0` the clock is detached from the wall clock, so a week of factory data is written to Parquet as fast as the machine allows. The router, the optional S3 filesystem and the start time stay in `parquet_logic.py`.

---

## Quick Start

Download the blueprint and the Python module beside it into the same folder, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/parquet.yaml
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/parquet_logic.py
```

### With uv

```bash
# 1. Run the blueprint
uv run --no-project --with "dynamic-des[parquet]" dynamic-des run parquet.yaml
```

### With pip

```bash
# 1. Install the package with the parquet extra
pip install "dynamic-des[parquet]"

# 2. Run the blueprint
dynamic-des run parquet.yaml
```

## What It Does

The run writes Parquet chunks to a local `data/` folder by default and ends on its own. On a laptop a week of simulated time takes about 20 seconds and writes about 850 files with 3.6 million rows, one row per lifecycle event. The router drops telemetry and flattens each event, so the columns are `stream_type`, `sim_ts`, `timestamp`, `key`, `path_id` and `status`.

To write to S3 instead, run `odctl up storage` and set `USE_S3=true`. The chunks land under the `odctl-dev/history/` prefix. `DEST_PATH`, `S3_ENDPOINT`, `S3_ACCESS_KEY` and `S3_SECRET_KEY` override the destination and credentials, exactly as for the Python example, because `parquet_logic.py` reads them.

## Full Source Code

`run.before` calls `ensure_destination`, so the folder or bucket is created only when the run starts, never when the file is loaded. The `telemetry` entry needs no Python, because every metric it publishes is a built-in resource statistic.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/parquet.yaml"
# Historical data generation in YAML.
#
# The twin of examples/declarative/parquet_example.py. With factor 0 the clock is
# detached from the wall clock, so a week of factory data is written to Parquet as
# fast as the machine allows. The router, the optional S3 filesystem and the start
# time stay in parquet_logic.py beside this file.
#
# Writes to ./data by default. Set USE_S3=true to write to the odctl storage
# profile instead (odctl up storage).
# Run it with: dynamic-des run examples/yaml/parquet.yaml

simulation:
  sim_id: Line_A
  factor: 0.0
  random_seed: 42
  # A week before now, so the records are stamped as history.
  logical_start_time: !python parquet_logic.LOGICAL_START_TIME

egress:
  - type: Parquet
    config:
      path_router: !python parquet_logic.router
      filesystem: !python parquet_logic.filesystem

batching:
  batch_size: 5000
  flush_interval: 86400

resources:
  lathe: {current_cap: 4, max_cap: 10}

services:
  milling: {dist: normal, mean: 2.0, std: 0.2}

arrivals:
  standard: {dist: exponential, rate: 2.0, spawn: process_part}

tasks:
  process_part:
    service: milling
    resource: lathe
    payload: {path_id: Line_A.service.milling, status: finished}

telemetry:
  - interval: 60.0
    publish:
      lathe.capacity: lathe.capacity
      lathe.in_use: lathe.in_use
      lathe.queue_length: lathe.queue_length
      lathe.utilization: lathe.utilization

run:
  until: 1 week
  before:
    - !python parquet_logic.ensure_destination
```

```python title="examples/yaml/parquet_logic.py"
"""Python for examples/yaml/parquet.yaml: the router, the destination and the start time.

Importing this module creates nothing. The destination is created by
ensure_destination, which the YAML calls through run.before.
"""

import logging
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def create_history_router(base_path: str):
    """
    Router Factory: Generates a router function injected with the correct
    base path, and flattens nested event payloads for Parquet.
    """

    def history_router(data: dict) -> str | None:
        if data.get("path_id") == "system.simulation.lag_seconds":
            return None

        stream_type = data.get("stream_type")

        if stream_type == "telemetry":
            return None

        # FLATTEN EVENT FOR PARQUET
        if (
            stream_type == "event"
            and "value" in data
            and isinstance(data["value"], dict)
        ):
            nested_value = data.pop("value")
            data.update(nested_value)

        return f"{base_path}/events.parquet"

    return history_router


use_s3 = os.getenv("USE_S3", "false").lower() == "true"
# odctl-dev is one of the buckets the odctl `storage` profile creates.
base_path = os.getenv("DEST_PATH", "odctl-dev/history" if use_s3 else "data")
filesystem = None

if use_s3:
    from pyarrow import fs

    filesystem = fs.S3FileSystem(
        access_key=os.getenv("S3_ACCESS_KEY", "user"),
        secret_key=os.getenv("S3_SECRET_KEY", "password"),
        endpoint_override=os.getenv("S3_ENDPOINT", "127.0.0.1:8333"),
        scheme="http",
    )


def ensure_destination() -> None:
    """Create the target directory or bucket path, once the run is about to start."""
    if use_s3 and filesystem is not None:
        logger.info(f"Configuring S3 Egress. Target Bucket: '{base_path}'")
        filesystem.create_dir(base_path)
    else:
        logger.info(f"Configuring Local Egress. Target Folder: '{base_path}'")
        os.makedirs(base_path, exist_ok=True)


router = create_history_router(base_path)

LOGICAL_START_TIME = datetime.now() - timedelta(days=7)
```
