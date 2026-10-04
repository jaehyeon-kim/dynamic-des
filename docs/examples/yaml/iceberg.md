# Fast-Forward to Iceberg (YAML)

This example builds the simulation of the [declarative Iceberg example](../declarative/iceberg.md) from a YAML blueprint, with no Python. A day of factory data is generated at `factor: 0.0`, and each flush of the buffer becomes one Iceberg commit.

---

## Quick Start

Download the blueprint, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/iceberg.yaml
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

The run appends a day of lifecycle events to the `simulation.events` table and ends on its own. `batch_size: 200000` on the egress makes each flush one commit, so the day lands in a few snapshots. Without a router, each event is written to `default_table` as one flat row, and telemetry is left out.

The catalog is contacted on the first write, not when the blueprint is loaded, so a blueprint with a wrong catalog address still loads and fails when the first batch is written.

`ICEBERG_URI`, `ICEBERG_WAREHOUSE`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_REGION` override the catalog and credentials.

## Full Source Code

`catalog` is a mapping of PyIceberg catalog properties, passed to PyIceberg's `load_catalog`. `schemas` names a type for each column, so `timestamp` is created as a timestamp rather than inferred as a string, and the ISO time each record carries is converted before the write.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/iceberg.yaml"
# Lakehouse data generation in YAML.
#
# The YAML version of examples/declarative/iceberg_example.py. A day of factory
# data is generated at factor 0, and each flush of the buffer becomes one Iceberg
# commit. Without a router, each event is written to default_table as one flat row
# and telemetry is left out. The catalog is contacted on the first write.
#
# Needs the odctl catalog profile: odctl up catalog. ICEBERG_URI, ICEBERG_WAREHOUSE,
# S3_ENDPOINT, S3_ACCESS_KEY and S3_SECRET_KEY override the catalog and credentials.
# Run it with: dynamic-des run examples/yaml/iceberg.yaml

simulation:
  sim_id: Line_A
  factor: 0.0
  random_seed: 42
  # A day before now, so the records are stamped as history.
  logical_start_time: -1d

egress:
  - type: Iceberg
    config:
      # PyIceberg catalog properties, passed to load_catalog.
      catalog:
        name: odctl
        type: rest
        uri: ${ICEBERG_URI:-http://localhost:8181}
        warehouse: ${ICEBERG_WAREHOUSE:-s3://warehouse/}
        s3.endpoint: ${S3_ENDPOINT:-http://localhost:8333}
        s3.access-key-id: ${S3_ACCESS_KEY:-user}
        s3.secret-access-key: ${S3_SECRET_KEY:-password}
        s3.region: ${S3_REGION:-us-east-1}
      default_table: simulation.events
      # Pinned rather than inferred, so timestamp is a timestamp, not a string.
      schemas:
        simulation.events:
          sim_ts: double
          timestamp: timestamp
          key: string
          path_id: string
          status: string
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
