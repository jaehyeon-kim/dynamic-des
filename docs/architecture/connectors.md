# Ingress and Egress Connectors

Connectors are the integration gateways of Dynamic DES. They handle the communication flow between the internal simulation registry/event queue and external systems.

---

## 1. Ingress Connectors (Inputs)

Ingress connectors listen to external sources and dynamically apply modifications to the simulation registry during runtime.

### Local Ingress (`LocalIngress`)
Applies scheduled overrides after set delays in wall-clock seconds from the start of the run. It waits with `asyncio.sleep`, so the delays do not follow simulation time: at `factor=0` the run can finish before the first change, and at any other factor the simulation time of each change varies a little from run to run. For changes at exact simulation times, use a [YAML scenario](#scenarios-versus-localingress).
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

### Scenarios versus `LocalIngress`
A [YAML blueprint](yaml.md) can carry a `scenario`: a list of registry changes, each with the simulation time it applies at, such as `{at: 10, path: Line_A.resources.lathe.current_cap, value: 3}`. [Script an experiment](../guides/yaml-blueprints.md#2-script-an-experiment) shows a complete file.

A scenario is not a connector. It is compiled into a SimPy process that waits on the simulation clock, so each change lands at exactly its `at`, on every run and at any `factor`, including `factor=0`. Every path is checked against the registry when the file is loaded, so a misspelt path stops the load with its line number. `LocalIngress` waits on the wall clock instead, and an unknown path is only logged as a warning when it is due.

A scenario and an ingress connector can be used together. Both write to the same registry, so a scripted baseline can run while an operator steers over Kafka. When both change the same path, the later write wins.

---

## 2. Egress Connectors (Outputs)

### Record shape
Every egress provider, router and `when` predicate receives records of two shapes. Telemetry, from `context.publish` or `publish_telemetry`:

```json
{"stream_type": "telemetry", "sim_ts": 12.0, "timestamp": "2026-01-01T00:00:12.000", "path_id": "Line_A.lathe.in_use", "value": 2}
```

Events, from `@app.task` or `publish_event`:

```json
{"stream_type": "event", "sim_ts": 12.5, "timestamp": "2026-01-01T00:00:12.500", "key": "task-7", "value": {"path_id": "Line_A.service.milling", "status": "started"}}
```

`sim_ts` is simulation seconds. `timestamp` is `logical_start_time` plus `sim_ts`, as an ISO string with milliseconds and no time zone. `context.publish` prefixes the metric name with the `sim_id`. Every run with an egress also publishes `system.simulation.lag_seconds` once per simulation second: how far the simulation clock is behind the wall clock. A router that writes only events has to drop it.

Egress connectors consume the simulation's event stream, serialize payloads, and dispatch them to downstream consumers.

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
The maximum number of simulation seconds to wait before flushing the memory buffer, even if `batch_size` has not been reached. At `factor=0` the timer is not started, because simulation time is detached from the wall clock, until a `go_live_at` switches the run to real time.
* **Tuning Guide**: In real-time mode (`factor=1.0`), set this to a low value (e.g. `0.5` or `1.0` seconds) to keep downstream UI dashboards responsive.

### Per-provider cadence

`with_batching` sets the default, and both values can be overridden per sink by passing them on `add_egress`. Each provider buffers separately, so memory is the sum of the buffers. See [Backfill Then Go Live in One Run](../guides/backfill-then-live.md).

The two limits are an OR, so the effective batch is the smaller of `batch_size` and what arrives within `flush_interval`. A high size with a short interval means the size never governs, which is reported once per run.
