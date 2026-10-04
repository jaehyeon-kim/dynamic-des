# Fast-Forward to Parquet (YAML)

This example builds the simulation of the [declarative Parquet example](../declarative/parquet.md) from a YAML blueprint, with no Python. With `factor: 0.0` the clock is detached from the wall clock, so a week of factory data is written to Parquet as fast as the machine allows.

---

## Quick Start

Download the blueprint, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/parquet.yaml
```

### With uv

```bash
# 1. Run the blueprint
uv run --no-project --with "dynamic-des[parquet]" ddes run parquet.yaml
```

### With pip

```bash
# 1. Install the package with the parquet extra
pip install "dynamic-des[parquet]"

# 2. Run the blueprint
ddes run parquet.yaml
```

## What It Does

The run writes Parquet chunks to a local `data/` folder by default and ends on its own. The folder is created on the first write. On a laptop a week of simulated time takes about 20 seconds and writes about 850 files with 3.6 million rows, one row per lifecycle event. Without a router, the egress drops telemetry and writes each event as one flat row, so the columns are `stream_type`, `sim_ts`, `timestamp`, `key`, `path_id` and `status`. `logical_start_time: -7d` stamps the week as history that ends now.

To write to S3 instead, run `odctl up storage` and set the variables the file reads:

```bash
PARQUET_FILESYSTEM=s3 DEST_PATH=odctl-dev/history S3_ENDPOINT=http://localhost:8333 \
  S3_ACCESS_KEY=user S3_SECRET_KEY=password ddes run parquet.yaml
```

The chunks land under the `odctl-dev/history/` prefix.

## Full Source Code

`filesystem` is a mapping. `type` picks the local disk or S3, and the other keys are passed to PyArrow's `S3FileSystem`. A key whose value is empty is left out, so with no variables set the mapping is the local disk.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml title="examples/yaml/parquet.yaml"
# Historical data generation in YAML.
#
# The YAML version of examples/declarative/parquet_example.py. With factor 0 the
# clock is detached from the wall clock, so a week of factory data is written to
# Parquet as fast as the machine allows. Without a router, each event is written as
# one flat row and telemetry is left out. The folder is created on the first write.
#
# Writes to ./data by default. To write to the odctl storage profile instead (odctl
# up storage), set PARQUET_FILESYSTEM=s3, DEST_PATH=odctl-dev/history,
# S3_ENDPOINT=http://localhost:8333, S3_ACCESS_KEY=user and S3_SECRET_KEY=password.
# Run it with: ddes run examples/yaml/parquet.yaml

simulation:
  sim_id: Line_A
  factor: 0.0
  random_seed: 42
  # A week before now, so the records are stamped as history.
  logical_start_time: -7d

egress:
  - type: Parquet
    config:
      default_path: ${DEST_PATH:-data}/events.parquet
      # Empty values are left out, so with no variables set this is the local disk.
      filesystem:
        type: ${PARQUET_FILESYSTEM:-local}
        endpoint_override: ${S3_ENDPOINT:-}
        access_key: ${S3_ACCESS_KEY:-}
        secret_key: ${S3_SECRET_KEY:-}

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

run:
  until: 1 week
```
