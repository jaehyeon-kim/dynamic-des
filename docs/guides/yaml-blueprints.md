# YAML Blueprints, from First File to Connectors

A blueprint puts the configuration of a simulation in a YAML file: the machines, the distributions, the connectors and the experiment. This guide starts with a first file, then adds a scripted experiment, connectors, and settings that change from one environment to the next. Every file on this page is plain YAML.

The [YAML Blueprints reference](../architecture/yaml.md) lists every section and field. Every file on this page is in the repository under [`docs/snippets/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/docs/snippets), and the tests build each one.

---

## When to use a blueprint

Use a blueprint when the configuration is what changes between runs: a new arrival rate, a second machine, a different broker, a capacity experiment. A YAML file is easier to review and to diff than a builder chain, and a run is one command.

Stay in Python when most of the program is logic. A blueprint can also reference Python objects with `!python` for the logic YAML cannot express, as [Advanced YAML: Custom Logic with `!python`](yaml-advanced.md) explains.

---

## 1. Write a first blueprint

The smallest useful blueprint declares a machine, how long it works, how often parts arrive, and what happens to each part:

```yaml title="docs/snippets/yaml/first.yaml"
# A first blueprint: one machine, one kind of part, printed to the terminal.
simulation:
  sim_id: Workshop
  # 0 runs as fast as the machine allows, so the run ends at once.
  factor: 0
  random_seed: 1

egress:
  - type: Console

resources:
  drill: {current_cap: 1, max_cap: 3}

services:
  drilling: {dist: normal, mean: 4.0, std: 1.0}

arrivals:
  parts: {dist: exponential, rate: 0.2, spawn: drill_part}

tasks:
  drill_part:
    service: drilling
    resource: drill
    payload: {status: finished}
    id_field: part_id

run:
  until: 60
```

Run it with the command the package installs:

```bash
dynamic-des run first.yaml
```

Each section maps to one builder call. `resources` is `add_resource`, `services` is `add_service`, and `arrivals` is `add_arrival`. `spawn: drill_part` starts the `drill_part` task on every arrival, which is the arrival loop a Python script writes by hand. The task requests the drill, waits a sampled drilling time and publishes its `payload`:

```text
[EVT] {'sim_ts': 5.365, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Workshop.service.drilling', 'status': 'queued'}}
[EVT] {'sim_ts': 5.365, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Workshop.service.drilling', 'status': 'started'}}
[EVT] {'sim_ts': 8.062, 'timestamp': '...', 'key': 'task-0', 'value': {'status': 'finished', 'part_id': 0}}
```

`factor: 0` runs as fast as the machine allows, so 60 simulation seconds end at once. Set `factor: 1` to pace the run in real time. A mistake stops the load with the file and the line, before anything runs:

```text
Error: first.yaml:22: task 'drill_part' uses service 'drill', which is not defined under services
```

---

## 2. Script an experiment

A `scenario` changes registry values at set simulation times. Here a second drill comes online at 30 seconds, parts arrive twice as fast from one minute, and the extra drill is removed at two minutes:

```yaml title="docs/snippets/yaml/scenario.yaml"
# The first blueprint with a scripted experiment: a second drill comes online at
# 30 s, parts arrive twice as fast from 1 min, and the extra drill is removed at
# 2 min. Every change lands at exactly that simulation time, on every run.
simulation:
  sim_id: Workshop
  factor: 0
  random_seed: 1

egress:
  - type: Console

resources:
  drill: {current_cap: 1, max_cap: 3}

services:
  drilling: {dist: normal, mean: 4.0, std: 1.0}

arrivals:
  parts: {dist: exponential, rate: 0.2, spawn: drill_part}

tasks:
  drill_part:
    service: drilling
    resource: drill
    payload: {status: finished}
    id_field: part_id

telemetry:
  # Shows each change as it lands.
  - interval: 10
    publish:
      drill.capacity: drill.capacity
      drill.queue_length: drill.queue_length

scenario:
  - {at: 30, path: Workshop.resources.drill.current_cap, value: 2}
  - {at: 1 min, path: Workshop.arrival.parts.rate, value: 0.4}
  - {at: 2 min, path: Workshop.resources.drill.current_cap, value: 1}

run:
  until: 3 min
```

The telemetry entry shows the change land:

```text
[TEL] {'sim_ts': 20.0, 'timestamp': '...', 'path_id': 'Workshop.drill.capacity', 'value': 1}
[TEL] {'sim_ts': 60.0, 'timestamp': '...', 'path_id': 'Workshop.drill.capacity', 'value': 2}
```

The steps run on the simulation clock, so the experiment repeats exactly on every run and works at `factor: 0`. Every path is checked when the file is loaded. A misspelt path such as `Workshop.resources.dril.current_cap` stops the load with its line, rather than being ignored at the moment it is due.

A path names one value in the registry: `<sim_id>.resources.<name>.current_cap`, `<sim_id>.arrival.<name>.rate` for an exponential arrival, `<sim_id>.service.<name>.mean`, or `<sim_id>.variables.<name>`. A scenario can run beside an ingress connector, so a scripted baseline can run while an operator steers over Kafka. [Scenarios versus `LocalIngress`](../architecture/connectors.md#scenarios-versus-localingress) explains why a scenario is not a `LocalIngress` schedule.

---

## 3. Connect to the outside

Connectors are listed under `ingress` and `egress`. `type` is a short name, and `config` holds the constructor's arguments. This is the Redis example, which writes to a stream and takes updates from a channel:

```yaml title="examples/yaml/redis.yaml"
# Redis Streams output with live parameter updates, in YAML.
#
# The YAML version of examples/declarative/redis_example.py. Each part_arrival
# spawns record_part, a task with no service or resource, which publishes its
# payload at once. RedisEgress writes it to the part_events stream that the
# __stream__ key names, while RedisIngress subscribes to the simulation_params
# channel.
#
# Needs Valkey: odctl up valkey. Runs until interrupted with Ctrl + C.
# Run it with: dynamic-des run examples/yaml/redis.yaml

simulation:
  sim_id: Factory
  factor: 1.0

ingress:
  - type: Redis
    config:
      # The odctl valkey profile disables the unauthenticated default user, so the
      # URL carries credentials.
      url: redis://user:password@localhost:6379/0
      channel_name: simulation_params

egress:
  - type: Redis
    config:
      url: redis://user:password@localhost:6379/0
      stream_name: events

arrivals:
  part_arrival: {dist: exponential, rate: 2.0, spawn: record_part}

tasks:
  record_part:
    # id_field adds the task id as part_id.
    payload: {__stream__: part_events, type: A, status: arrived}
    id_field: part_id
```

The other options of `add_egress` sit beside `config`: `when` routes records, and `batch_size` and `flush_interval` give one sink its own cadence. `when: history` sends the records stamped before `simulation.go_live_at`, and `when: live` sends the rest. `batching` sets the defaults, as `with_batching` does. The [YAML examples](../examples/yaml/local.md) cover every connector the declarative examples use: Console, Kafka, Parquet, Iceberg, Postgres and Redis, and the backfill-then-live run that sends history to Parquet and the live tail to Kafka.

Settings that the Python API takes as objects are plain values in `config`:

- A Parquet or JSONL `filesystem` is a mapping. `type` is `local` or `s3`, and the other keys are passed to PyArrow's `S3FileSystem`. A key whose value is empty is left out. The destination folder is created on the first write.
- An Iceberg `catalog` is a mapping of PyIceberg catalog properties, and `schemas` names a type for each column, such as `string`, `double` or `timestamp`.
- Without a router, Parquet, JSONL and Iceberg write each event to `default_path` or `default_table` as one flat row, and leave telemetry out.
- Kafka creates its event and telemetry topics, and Postgres creates the tables listed under `tables`, when the run starts.

---

## 4. Change settings per environment

`${VAR}` in a value is replaced with the environment variable, and `${VAR:-default}` uses the default when the variable is unset or empty. An unquoted value is read as YAML reads it after the replacement, so `port: ${PG_PORT:-5432}` is the number 5432. `$${` writes a literal `${`. The Kafka example reads its broker this way, as `bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}`, and the [Parquet example](../examples/yaml/parquet.md) switches from the local disk to S3 with five variables.

`logical_start_time` and `go_live_at` take `now`, a signed duration from now such as `-1d` or `-10m`, or an ISO datetime. Both are read against one moment, when the file is loaded, so `-10m` and `now` are exactly ten minutes apart. The [backfill-then-live example](../examples/yaml/backfill-live.md) uses both, with `when: history` and `when: live`.

---

## 5. Limitations

A blueprint is configuration. These stay in Python:

- **Field values with logic.** A mapping payload is a constant, apart from the task id that `id_field` adds. Random or weighted choices, derived fields and state carried between events are a Python payload or process, as the [advanced guide](yaml-advanced.md) shows.
- **Containers and stores as SimPy objects.** `containers` registers capacities in the registry only, as `add_container` does. A process that needs a SimPy container builds a `DynamicContainer`, or reads and writes the registry path as the hot rolling twin does. Stores have no builder method, so neither API registers them.
- **Anything beyond one task on one resource.** Multi-stage routing, several resources per task, priorities and preemption are processes.
- **Telemetry other than resource statistics.** Container levels, variables and derived metrics need a telemetry `function`.

The [reference](../architecture/yaml.md#limitations) lists the full set.
