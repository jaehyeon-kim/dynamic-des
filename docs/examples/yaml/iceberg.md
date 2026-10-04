# Fast-Forward to Iceberg (YAML)

This example builds the same simulation as the [declarative Iceberg example](../declarative/iceberg.md) from a YAML blueprint. A day of factory data is generated at `factor: 0.0`, and each flush of the buffer becomes one Iceberg commit. The catalog client, the table router and the pinned schema stay in `iceberg_logic.py`.

---

## Quick Start

Download the blueprint and the Python module beside it into the same folder, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/iceberg.yaml
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/iceberg_logic.py
```

### With uv

```bash
# 1. Install odctl, which runs the containers
uv tool install "odctl>=0.5.1"

# 2. Start the Iceberg REST catalog
odctl up catalog

# 3. Run the blueprint
uv run --no-project --with "dynamic-des[iceberg]" dynamic-des run iceberg.yaml

# 4. Clean up the infrastructure when finished
odctl down catalog --volumes
```

### With pip

```bash
# 1. Install the package with the iceberg extra, and odctl for the containers
pip install "dynamic-des[iceberg]" "odctl>=0.5.1"

# 2. Start the Iceberg REST catalog
odctl up catalog

# 3. Run the blueprint
dynamic-des run iceberg.yaml

# 4. Clean up the infrastructure when finished
odctl down catalog --volumes
```

## What It Does

The run appends a day of lifecycle events to the `simulation.events` table and ends on its own. `batch_size: 200000` on the egress makes each flush one commit, so the day lands in a few snapshots rather than dozens.

The catalog is contacted when the blueprint is loaded, not when the run starts. `iceberg_logic.py` builds the `RestCatalog` client at import, and pyiceberg fetches the catalog configuration as the client is constructed, so without `odctl up catalog` the load stops with a `ConnectionError`, reported against the line of the first `!python iceberg_logic` reference.

`ICEBERG_URI`, `ICEBERG_WAREHOUSE`, `ICEBERG_NAMESPACE`, `S3_ENDPOINT`, `S3_ACCESS_KEY` and `S3_SECRET_KEY` override the catalog and credentials.

## Full Source Code

`schemas` takes a mapping of table name to a PyArrow schema, which YAML cannot build, so it is one `!python` reference to a dictionary in the module.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/iceberg.yaml"
# Lakehouse data generation in YAML.
#
# The twin of examples/declarative/iceberg_example.py. A day of factory data is
# generated at factor 0, and each flush of the buffer becomes one Iceberg commit.
# The catalog, the table router and the pinned schema stay in iceberg_logic.py
# beside this file.
#
# Needs the odctl catalog profile: odctl up catalog. The catalog is contacted when
# this file is loaded, because iceberg_logic.py builds the catalog client on import.
# Run it with: dynamic-des run examples/yaml/iceberg.yaml

simulation:
  sim_id: Line_A
  factor: 0.0
  random_seed: 42
  # A day before now, so the records are stamped as history.
  logical_start_time: !python iceberg_logic.LOGICAL_START_TIME

egress:
  - type: Iceberg
    config:
      catalog: !python iceberg_logic.catalog
      table_router: !python iceberg_logic.router
      schemas: !python iceberg_logic.SCHEMAS
    # One commit per flush, sized so a day of events lands in a few snapshots.
    batch_size: 200000

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

run:
  until: 1 day
```

```python title="examples/yaml/iceberg_logic.py"
"""Python for examples/yaml/iceberg.yaml: the catalog, the router and the schema."""

import os
from datetime import datetime, timedelta

import pyarrow as pa

NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "simulation")
EVENTS_TABLE = f"{NAMESPACE}.events"

# Pinned rather than inferred. Inference reads the ISO timestamp the environment
# writes as a string, which is not what a consumer of an event table expects.
EVENTS_SCHEMA = pa.schema(
    [
        ("sim_ts", pa.float64()),
        ("timestamp", pa.timestamp("us")),
        ("key", pa.string()),
        ("path_id", pa.string()),
        ("status", pa.string()),
    ]
)
SCHEMAS = {EVENTS_TABLE: EVENTS_SCHEMA}


def create_table_router(events_table: str):
    """
    Router Factory: Generates a router returning `namespace.table`, and reshapes
    each event into the pinned schema.
    """

    def table_router(data: dict) -> str | None:
        if data.get("path_id") == "system.simulation.lag_seconds":
            return None

        if data.get("stream_type") != "event":
            return None

        # FLATTEN EVENT INTO THE PINNED COLUMNS
        nested_value = data.pop("value", None)
        if isinstance(nested_value, dict):
            data.update(nested_value)

        # A pinned timestamp column takes a datetime. PyArrow rejects the ISO string
        # the environment writes, so the conversion belongs here.
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])

        return events_table

    return table_router


def build_catalog():
    """Connects to the Iceberg REST catalog from the odctl `catalog` profile."""
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "odctl",
        **{
            "uri": os.getenv("ICEBERG_URI", "http://localhost:8181"),
            "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
            "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:8333"),
            "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "user"),
            "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "password"),
            "s3.region": os.getenv("S3_REGION", "us-east-1"),
        },
    )


router = create_table_router(EVENTS_TABLE)

# RestCatalog fetches the catalog configuration when it is constructed, so this line
# contacts the catalog as the YAML file is loaded.
catalog = build_catalog()

LOGICAL_START_TIME = datetime.now() - timedelta(days=1)
```
