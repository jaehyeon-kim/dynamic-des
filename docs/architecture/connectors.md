# Ingress and Egress Connectors

Connectors are the integration gateways of Dynamic DES. They handle the communication flow between the internal simulation registry/event queue and external systems.

---

## 1. Ingress Connectors (Inputs)

Ingress connectors listen to external sources and dynamically apply modifications to the simulation registry during runtime.

### Local Ingress (`LocalIngress`)
Applies scheduled overrides at predetermined simulation timestamps. This is ideal for local testing, debugging, and executing deterministic test scenarios.
```python
from dynamic_des import LocalIngress

# At t=10.0s, set machine lathe capacity to 3
ingress = LocalIngress(schedule=[(10.0, "Line_A.resources.lathe.current_cap", 3)])
```

### Kafka Ingress (`KafkaIngress`)
Spawns a consumer in the background thread that listens to a Kafka control topic. External admin tools can write a command payload to the topic (e.g. updating the speed of a conveyor belt), and the connector automatically applies the change to the Registry in real time.

---

## 2. Egress Connectors (Outputs)

Egress connectors consume the simulation's event stream, serialize payloads, and dispatch them to downstream consumers.

### Console Egress (`ConsoleEgress`)
Prints formatted telemetry and event payloads to the system logger.

### Kafka Egress (`KafkaEgress`)
Streams telemetry and events in real time to designated Kafka topics.

### Storage Egress (`ParquetStorageEgress` / `JsonlStorageEgress`)
Writes records to compressed, chunked files using PyArrow. Writes to local directories and S3-compatible storage such as AWS S3 or SeaweedFS.

### PostgreSQL Egress (`PostgresEgress`)
Inserts records into a PostgreSQL table in batches, through `asyncpg`. By default a record whose key already exists is skipped. Pass `upsert_keys` to update that row instead, for simulations that change rows they wrote earlier, such as an order moving from processing to shipped:

```python
PostgresEgress(dsn, table_name="orders", upsert_keys=["order_id"])
```

The key columns need a unique index or constraint. An update sets only the columns the record carries, so a record with just the key and a new status leaves the other columns as they were. Within one flush, the last record for a key wins.

### Iceberg Egress (`IcebergStorageEgress`)
Appends records into an Apache Iceberg table through an Iceberg REST catalog the caller supplies. One flush is one commit, which is why this provider wants a large `batch_size` of its own: every commit writes a manifest, a manifest list and a new `metadata.json`, and query planning degrades as snapshots accumulate.

Pass `upsert_keys` to upsert a table on its key columns instead of appending, so rerunning a seeded simulation over the same window does not duplicate rows:

```python
IcebergStorageEgress(catalog=catalog, table_router=router, upsert_keys={"sim.orders": ["order_id"]})
```

An upsert reads the matching rows before writing, so it is slower than an append, and it never deletes rows. It replaces the whole row, so every record must carry every column. The option needs pyiceberg 0.9.0 or later. Within one flush, the last record for a key wins. An upsert flush is still one commit, but it can add up to three snapshots.

### Attaching more than one

Every attached provider receives every record. Each has its own queue, so attaching a stream sink and a lake sink writes the same dataset to both in a single run, rather than needing one run per sink with a matching seed.

```python
app.add_egress(kafka).add_egress(parquet)
```

Pass `when` to route records instead of duplicating them. The predicate takes one record and returns True to send it to that provider, which reads the same way as the `path_router` that `ParquetStorageEgress` already accepts:

```python
app.add_egress(kafka,   when=lambda r: r["timestamp"] >= hot_from)  # hot tail
app.add_egress(parquet, when=lambda r: r["timestamp"] <  hot_from)  # cold history
app.add_egress(audit)                                               # no predicate: everything
```

A provider with no predicate receives everything, so the two can be mixed. Records matching no predicate are simply not written anywhere, which is how you drop them.

---

## Tuning I/O Efficiency

Both the environment and the connectors support tuning for optimal throughput and network usage:

### `batch_size`
The maximum number of events to buffer in memory before triggering a flush.
* **Tuning Guide**: In fast-forward batch mode (`factor=0.0`), set this to a high value (e.g. `5000` or `10000`) to maximize write throughput and produce highly compressed Parquet chunks.

### `flush_interval`
The maximum number of seconds to wait before flushing the memory buffer, even if `batch_size` has not been reached.
* **Tuning Guide**: In real-time mode (`factor=1.0`), set this to a low value (e.g. `0.5` or `1.0` seconds) to keep downstream UI dashboards responsive.

### Per-provider cadence

`with_batching` sets the default, and both values can be overridden per sink by passing them on `add_egress`. Each provider buffers separately, so memory is the sum of the buffers. See [Backfill Then Go Live in One Run](../guides/backfill-then-live.md).

The two limits are an OR, so the effective batch is the smaller of `batch_size` and what arrives within `flush_interval`. A high size with a short interval means the size never governs, which is reported once per run.
