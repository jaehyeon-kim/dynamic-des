# Fast-Forward to Data Lake (Low-Level Imperative API)

While `dynamic-des` is designed for real-time digital twins, it is equally powerful as a **synchronized forecasting engine**. By manipulating the environment's time factor and initial state, you can run simulations to generate vast amounts of historical data or instantly predict future states.

This example demonstrates how to run a simulation in **fast-forward mode** using the low-level **Imperative API** and write compressed columnar data (Parquet) directly to local storage or an AWS S3 data lake using the `ParquetStorageEgress` connector.

---

## Quick Start

The run writes Parquet chunks to a local `data/` folder by default, so no
infrastructure is needed:

```bash
uv run ddes-imperative-history
```

To write to S3 instead, start the object store and set `USE_S3`. The chunks land
under the `des-dev/history/` prefix, browsable at <http://localhost:8889>:

```bash
# 1. Spin up SeaweedFS via Docker Compose
uv run ddes-storage-infra-up

# 2. Run the simulation against S3
USE_S3=true uv run ddes-imperative-history

# 3. Clean up the infrastructure when finished
uv run ddes-storage-infra-down
```

`DEST_PATH`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` override the
destination and credentials.

## Full Source Code

This script simulates a manufacturing line over a 7-day period. It demonstrates how to route lifecycle events to one Parquet dataset, drop real-time metrics, and write them out.

```python
--8<-- "src/dynamic_des/examples/imperative/history_example.py"
```
