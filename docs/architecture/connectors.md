# Connectors

Connectors are the integration gateways of Dynamic DES. They handle the communication flow between the internal simulation registry/event queue and external systems.

---

## 1. Ingress Connectors (Inputs)

Ingress connectors listen to external sources and dynamically apply modifications to the simulation registry during runtime. [Registry and Live Parameters](registry.md) explains how an update reaches the simulation.

### Local Ingress (`LocalIngress`)
Applies scheduled overrides after set delays in wall-clock seconds from the start of the run. It waits with `asyncio.sleep`, so the delays do not follow simulation time: at `factor=0` the run can finish before the first change, and at any other factor the simulation time of each change varies a little from run to run. For changes at exact simulation times, use a [YAML scenario](registry.md#scenarios-versus-localingress).
```python
from dynamic_des import LocalIngress

# 10 wall-clock seconds after the start, set machine lathe capacity to 3
ingress = LocalIngress(schedule=[(10.0, "Line_A.resources.lathe.current_cap", 3)])
```

### Kafka Ingress (`KafkaIngress`)
Spawns a consumer in the background thread that listens to a Kafka control topic. External admin tools can write a command payload to the topic (e.g. updating the speed of a conveyor belt), and the connector automatically applies the change to the Registry in real time. Each message is a JSON object with `path_id` and `value`, such as `{"path_id": "Line_A.resources.lathe.current_cap", "value": 3}`.

### Redis Ingress (`RedisIngress`)
Subscribes to a Redis Pub/Sub channel. Each message is a JSON object with `param_path` and `param_value`, such as `{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}`.

### PostgreSQL Ingress (`PostgresIngress`)
Polls a table of parameter updates, every 2 seconds by default (`poll_interval`). At start it replays the latest applied value of each path, so a restarted simulation resumes from the last settings. It then applies every row whose `is_applied` is FALSE and marks it TRUE, so the table keeps the history of every change. The connector creates the table if it is missing.

---

## 2. Egress Connectors (Outputs)

Egress connectors consume the simulation's event stream, serialize payloads, and dispatch them to downstream consumers. [Records and Telemetry](records.md) shows the records every egress receives, and [Batching and Delivery](batching.md) explains how they reach each one.

### Console Egress (`ConsoleEgress`)
Prints formatted telemetry and event payloads to the system logger.

### Kafka Egress (`KafkaEgress`)
Streams telemetry and events in real time to designated Kafka topics.

### Storage Egress (`ParquetStorageEgress` / `JsonlStorageEgress`)
Writes records to chunked files using PyArrow: compressed Parquet, or JSON Lines. Writes to local directories and S3-compatible storage such as AWS S3 or SeaweedFS.

### Redis Egress (`RedisEgress`)
Appends each record to a Redis Stream with `XADD`, as one `payload` field holding the record as JSON. A `__stream__` key inside the record's value picks the stream, and records without one go to `stream_name`.

### PostgreSQL Egress (`PostgresEgress`)
Inserts records into a PostgreSQL table in batches, through `asyncpg`. Only records whose value is a dictionary are written. The record's `sim_ts`, `timestamp`, and its `key` (as `event_id`) or `path_id` are merged into the value, and keys with no matching column are dropped. A `__table__` key in the value sends the record only to the instance whose `table_name` matches, which is how one stream fills several tables. By default a record whose key already exists is skipped. Pass `upsert_keys` to update that row instead, for simulations that change rows they wrote earlier, such as an order moving from processing to shipped:

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
