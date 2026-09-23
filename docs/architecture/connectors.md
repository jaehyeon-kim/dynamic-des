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
Writes records to compressed, chunked files using PyArrow. Natively supports S3-compatible endpoints, AWS S3, Google Cloud Storage, and local directories.

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
