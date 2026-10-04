# YAML Blueprints

A YAML blueprint is a third way to define a simulation, beside the `SimulationContext` builder and the low-level `DynamicRealtimeEnvironment`. The file holds the configuration: parameters, connectors, batching, simple tasks, telemetry and a scenario of timed changes. Logic that YAML cannot express is Python, which a blueprint references with `!python`, as [Advanced YAML: Custom Logic with `!python`](../guides/yaml-advanced.md) explains.

A blueprint is built through the same public builder methods a Python script calls, such as `add_resource`, `add_egress`, `task` and `arrival_loop`. There is no second build path, so a blueprint and a script that make the same calls produce the same simulation. The tests build and run each [YAML example](../examples/local.md) and check the records it produces.

For a walk from a first blueprint to live connectors, see [YAML Blueprints, from First File to Connectors](../guides/yaml-blueprints.md). This page is the reference.

---

## Example

A blueprint declares the same configuration as the builder chain in a YAML file, and `ddes run` builds and runs it. Simple tasks, arrival loops and resource telemetry are declared in the file. Processes, payload functions and routers are Python, referenced with `!python`. The [local example](../examples/local.md) needs no Python at all.

```yaml title="examples/yaml/local.yaml"
# Local simulation in YAML, with no Python and no containers.
#
# The twin of examples/declarative/local_example.py. Factory_A writes to
# ConsoleEgress, so events and telemetry are printed to the terminal, and the run
# ends on its own after 60 simulation seconds.
#
# Run it with: ddes run examples/yaml/local.yaml

simulation:
  sim_id: Factory_A
  factor: 1.0

egress:
  - type: Console

resources:
  lathe: {current_cap: 2, max_cap: 5}

services:
  milling: {dist: normal, mean: 3.0, std: 0.5}

arrivals:
  # Each arrival spawns one process_part task.
  standard: {dist: exponential, rate: 1.0, spawn: process_part}

tasks:
  process_part:
    service: milling
    resource: lathe
    # The value of the task's finished event. id_field adds the task id as part_id.
    payload: {event_type: part_produced, quality: A}
    id_field: part_id

telemetry:
  # Samples the lathe every 2 simulation seconds.
  - interval: 2.0
    publish:
      utilization: lathe.utilization
      queue_length: lathe.queue_length

run:
  until: 60
```

---

## Running a Blueprint

From a shell, with the `ddes` command that the package installs:

```bash
ddes run blueprint.yaml               # runs until run.until, or forever
ddes run blueprint.yaml --until 10min # overrides run.until
ddes --version
```

`--until` takes seconds or a duration such as `10 min`, `2 hours` or `1 week`, the forms `time_to_seconds` accepts. The command logs at INFO level, so `ConsoleEgress` output is visible. An invalid file prints its errors and exits with code 1.

From Python:

```python
from dynamic_des import SimulationContext

app = SimulationContext.from_yaml("blueprint.yaml")
app.run()  # calls run.before, then runs until run.until
```

`from_yaml` returns an ordinary `SimulationContext`, so the builder methods and decorators still work on it. Passing `until` to `run()` overrides `run.until`.

---

## Sections

Every section is optional except `simulation`. Unknown keys are rejected, so a misspelt key is reported rather than ignored.

| Section | Builds with | Contents |
|---|---|---|
| `simulation` | `SimulationContext(...)` | `sim_id` (required), `factor`, `random_seed`, `logical_start_time`, `go_live_at` |
| `ingress` | `add_ingress` | A list of connectors, each `type` and `config` |
| `egress` | `add_egress` | A list of connectors, each `type`, `config`, and optionally `when`, `batch_size`, `flush_interval`, both greater than 0 |
| `batching` | `with_batching` | `batch_size`, `flush_interval`, and optionally `max_queued_batches`, `drain_stall_seconds`, all greater than 0 |
| `resources` | `add_resource` | `name: {current_cap, max_cap}`, whole numbers |
| `containers` | `add_container` | `name: {current_cap, max_cap}` |
| `variables` | `add_variable` | `name: value`, any YAML value |
| `services` | `add_service` | `name: {dist, mean, std, rate}` |
| `arrivals` | `add_arrival` | `name: {dist, mean, std, rate, spawn}` |
| `tasks` | `task` | `name: {service, resource, payload, id_field}` |
| `processes` | `add_process` | A list of Python generator functions, each optionally with `kwargs` |
| `telemetry` | `telemetry_loop` | A list of `{interval, publish}` or `{interval, function}`, with `interval` greater than 0 |
| `scenario` | `add_process` | A list of `{at, path, value}` |
| `run` | `run()` | `until`, greater than 0, and `before`, a list of Python functions |

`dist` is `exponential`, `normal` or `lognormal`. An exponential distribution reads `rate`, and the other two read `mean` and `std`. A field left out takes the builder's default, so a blueprint registers exactly what the equivalent Python call registers.

`logical_start_time` and `go_live_at` take `now`, a signed duration from now such as `-1d` or `-10m`, or an ISO datetime such as `2026-01-01T00:00:00`. Both are read against one moment, when the file is loaded, so `-10m` and `now` are exactly ten minutes apart. `now` and the relative forms have no time zone. `go_live_at` and `logical_start_time` must both have a time zone or both have none, and a file with no `logical_start_time` starts without one. `run.until` takes seconds or a duration string such as `11m`.

### Environment variables

`${VAR}` in a value is replaced with the environment variable, so one file can serve several environments. `${VAR:-default}` uses the default when the variable is unset or empty, as in a shell, and `$${` writes a literal `${`. An unquoted value is read as YAML reads it after the replacement, so `port: ${PG_PORT:-5432}` is the number 5432. A quoted value stays a string. A variable without a default that is unset stops the load with its line.

### Tasks and arrivals

A task is the `@app.task` decorator in YAML. Each run of the task emits `queued`, waits for the resource, emits `started`, waits for a service time sampled from the service, and emits a finished event whose value is the `payload`.

- `payload` is a mapping, published as it is. A payload computed for each task is a Python function, described in the [advanced guide](../guides/yaml-advanced.md).
- `id_field` adds the task id to a mapping payload under that key. It is the only part of a mapping payload that changes from one task to the next.
- A task with neither `service` nor `resource` takes no time and holds no resource. It emits its payload as soon as it is spawned, with no `queued` or `started` event.

`spawn` on an arrival names the task that each arrival starts. It is the loop a declarative example writes by hand:

```python
@app.arrival_loop("standard")
def arrival_generator(context):
    task_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(process_part(task_id, context))
        task_id += 1
```

An arrival without `spawn` registers only its distribution, for a process to sample with `context.wait_for_arrival`.

### Telemetry

A telemetry entry runs every `interval` simulation seconds. `publish` maps a metric name to `<resource>.<stat>`, where the stat is one of:

| Stat | Value |
|---|---|
| `capacity` | The resource's current capacity |
| `in_use` | Tokens held |
| `queue_length` | Requests waiting |
| `utilization` | `in_use / capacity * 100`, or 0 when the capacity is 0 |

Each metric is published as `<sim_id>.<metric name>`. For anything else, `function` takes a Python function instead of `publish`, as the [advanced guide](../guides/yaml-advanced.md) describes.

### Connectors

`type` is one of the short names below, or a connector class from another package, as the [advanced guide](../guides/yaml-advanced.md) describes. `config` holds the constructor's keyword arguments, so it takes exactly what the class takes.

| Direction | `type` | Class | Extra |
|---|---|---|---|
| egress | `Console` | `ConsoleEgress` | none |
| egress | `Kafka` | `KafkaEgress` | `kafka` |
| egress | `Parquet` | `ParquetStorageEgress` | `parquet` |
| egress | `Jsonl` | `JsonlStorageEgress` | `parquet` |
| egress | `Iceberg` | `IcebergStorageEgress` | `iceberg` |
| egress | `Postgres` | `PostgresEgress` | `postgres` |
| egress | `Redis` | `RedisEgress` | `redis` |
| ingress | `Local` | `LocalIngress` | none |
| ingress | `Kafka` | `KafkaIngress` | `kafka` |
| ingress | `Postgres` | `PostgresIngress` | `postgres` |
| ingress | `Redis` | `RedisIngress` | `redis` |

A connector module is imported only when a blueprint names it, so a blueprint that uses Kafka does not need the Postgres driver. When a connector's package is missing, the load stops with the line and the `pip install` command. Parquet and JSONL are the exception: they import PyArrow only when they start writing, so a missing `parquet` extra is reported when the run starts.

Settings that the Python API takes as objects are plain values:

| `type` | Setting | Plain value |
|---|---|---|
| `Kafka` | topics | Without a `topic_router`, `event_topic` and `telemetry_topic` are created when the run starts, with one partition each, if they do not exist. |
| `Parquet`, `Jsonl` | `filesystem` | A mapping. `type` is `local` (the default) or `s3`, and the other keys are passed to PyArrow's `S3FileSystem`, such as `endpoint_override`, `access_key`, `secret_key` and `region`. An `endpoint_override` starting with `http://` sets the scheme. A key whose value is empty is left out, so `${S3_ENDPOINT:-}` with the variable unset takes the default. |
| `Parquet`, `Jsonl` | `default_path` | Without a `path_router`, each event is written there as one flat row, with its `value` mapping unpacked into columns, and telemetry is left out. The folder is created on the first write. |
| `Iceberg` | `catalog` | A mapping of PyIceberg catalog properties, such as `type`, `uri`, `warehouse` and `s3.endpoint`, passed to `load_catalog` on the first write. An optional `name` key names the catalog. |
| `Iceberg` | `default_table`, `schemas` | Without a `table_router`, each event is written to `default_table` as one flat row. `schemas` maps a table to its columns, each with a type: `string`, `int`, `long`, `float`, `double`, `boolean`, `timestamp`, `timestamptz`, `date` or `binary`. |
| `Postgres` | `tables` | Tables created when the run starts, if they do not exist. Each maps to `columns`, a mapping of column name to SQL type, and an optional `primary_key`. `table_name` defaults to the table when `tables` names one. |

An ISO time string bound for a timestamp or date column of Iceberg or Postgres is converted before the write.

`when` on an egress entry is `history`, which sends the records stamped before `simulation.go_live_at`, or `live`, which sends the records stamped at or after it. Both need `go_live_at`.

### Scenario

A scenario sets registry paths to new values at given simulation times. Each step is a mapping such as `{at: 30, path: Workshop.resources.drill.current_cap, value: 2}`.

`at` is seconds from the start of the run, or a duration string. The steps are applied in time order by a SimPy process that waits on the simulation clock, so a scenario repeats exactly on every run and works at `factor: 0`. Every `path` is checked when the file is loaded, against a registry built from the blueprint with the same code the run uses, and the `value` must convert to the type the path holds. [Scenarios versus `LocalIngress`](registry.md#scenarios-versus-localingress) compares the two. The [guide](../guides/yaml-blueprints.md#2-script-an-experiment) has a complete file.

A resource follows a capacity change within the same simulation instant, but after the step that made it. A telemetry sample taken at exactly that instant can therefore still show the old capacity.

---

## Limitations

A blueprint covers configuration. Anything that is logic stays in Python.

- **No expression language for field values.** A mapping payload is a constant, apart from the task id that `id_field` adds. Weighted or Zipf choices, fields derived from other fields, values drawn from distributions, Markov walks and state carried from one event to the next are all Python, which the [advanced guide](../guides/yaml-advanced.md) covers. The [advanced Postgres example](../examples/advanced-postgres-orders.md) is like this: its generator draws random values, so it is a process in Python.
- **Containers and stores exist only in the registry.** `containers` registers capacities, as `add_container` does, but no SimPy container is created for them. A process that needs one builds a `DynamicContainer` itself, or reads and writes the registry path directly, as the hot rolling twin does with its wear levels. `SimParameter.stores` has no builder method, so neither the builder nor a blueprint can register stores.
- **Built-in telemetry reads resources only.** Container levels, variables or derived metrics need a telemetry `function`.
- **A task uses at most one service and one resource.** Multi-stage routing, several resources per task, priorities and preemption are processes in Python.
- **One simulation per file.** A blueprint has one `sim_id`.

---

## Errors

Every error names the file and the line, and `ddes run` prints it after `Error:`. Validation errors are all reported at once:

```text
Error: err.yaml:3: simulation.speed: Extra inputs are not permitted
err.yaml:9: arrivals.standard.dist: Input should be 'exponential', 'normal' or 'lognormal'
```

Cross-references are checked after validation: a task's `service` and `resource`, an arrival's `spawn`, each telemetry `publish` entry, and each scenario `path` and `value`.

```text
Error: err.yaml:11: task 'part' uses service 'drilling', which is not defined under services
Error: err.yaml:6: 'Line_A.resources.press.current_cap' is not a registry path. Paths look like Line_A.resources.<name>.current_cap or Line_A.arrival.<name>.rate
```

---

## Security

A blueprint with no Python references is read entirely by `yaml.SafeLoader`, and loading it imports only the connector modules its `type` entries name. A file that references Python runs the imports it names, so it is as trusted as a Python script. The [advanced guide](../guides/yaml-advanced.md#7-security) explains.
