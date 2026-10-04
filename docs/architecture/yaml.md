# YAML Blueprints

A YAML blueprint is a third way to define a simulation, beside the `SimulationContext` builder and the low-level `DynamicRealtimeEnvironment`. The file holds the configuration: parameters, connectors, batching, simple tasks, telemetry and a scenario of timed changes. Logic that is code stays in Python and is referenced from the file with `!python`.

A blueprint is built through the same public builder methods a Python script calls, such as `add_resource`, `add_egress`, `task` and `arrival_loop`. There is no second build path, so a blueprint and a script that make the same calls produce the same simulation. The examples prove this: each [YAML example](../examples/yaml/local.md) is tested against its declarative twin with the same seed, and the two must publish the same records.

For a walk from a first blueprint to a hybrid one, see [YAML Blueprints, from First File to Hybrid](../guides/yaml-blueprints.md). This page is the reference.

---

## Running a Blueprint

From a shell, with the `dynamic-des` command that the package installs:

```bash
dynamic-des run blueprint.yaml               # runs until run.until, or forever
dynamic-des run blueprint.yaml --until 10min # overrides run.until
dynamic-des --version
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
| `egress` | `add_egress` | A list of connectors, each `type`, `config`, and optionally `when`, `batch_size`, `flush_interval` |
| `batching` | `with_batching` | `batch_size`, `flush_interval`, and optionally `max_queued_batches`, `drain_stall_seconds` |
| `resources` | `add_resource` | `name: {current_cap, max_cap}`, whole numbers |
| `containers` | `add_container` | `name: {current_cap, max_cap}` |
| `variables` | `add_variable` | `name: value`, any YAML value |
| `services` | `add_service` | `name: {dist, mean, std, rate}` |
| `arrivals` | `add_arrival` | `name: {dist, mean, std, rate, spawn}` |
| `tasks` | `task` | `name: {service, resource, payload, id_field}` |
| `processes` | `add_process` | A list of `!python` generator functions, each optionally with `kwargs` |
| `telemetry` | `telemetry_loop` | A list of `{interval, publish}` or `{interval, function}` |
| `scenario` | `add_process` | A list of `{at, path, value}` |
| `run` | `run()` | `until`, and `before`, a list of `!python` functions |

`dist` is `exponential`, `normal` or `lognormal`. An exponential distribution reads `rate`, and the other two read `mean` and `std`. A field left out takes the builder's default, so a blueprint registers exactly what the equivalent Python call registers.

`logical_start_time` and `go_live_at` take a YAML timestamp such as `2026-01-01T00:00:00`, or a `!python` reference to a `datetime`. `run.until` takes seconds, a duration string, or a `!python` reference to a number.

### Tasks and arrivals

A task is the `@app.task` decorator in YAML. Each run of the task emits `queued`, waits for the resource, emits `started`, waits for a service time sampled from the service, and emits a finished event whose value is the `payload`.

- `payload` is a mapping, published as it is, or a `!python` function called as `payload(task_id, context)` that returns the value.
- `id_field` adds the task id to a mapping payload under that key. It is the only part of a mapping payload that changes from one task to the next.

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

Each metric is published as `<sim_id>.<metric name>`. For anything else, give `function: !python module.func` instead of `publish`. It is called as `func(context)`, as a function decorated with `@app.telemetry_loop` is.

### Connectors

`type` is one of the short names below, or a `!python` reference to a connector class from another package. `config` holds the constructor's keyword arguments, so it takes exactly what the class takes. Values that are Python objects, such as a router or a filesystem, are `!python` references.

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

### Scenario

A scenario sets registry paths to new values at given simulation times. Each step is a mapping such as `{at: 30, path: Workshop.resources.drill.current_cap, value: 2}`.

`at` is seconds from the start of the run, or a duration string. The steps are applied in time order by a SimPy process that waits on the simulation clock, so a scenario repeats exactly on every run and works at `factor: 0`. Every `path` is checked when the file is loaded, against a registry built from the blueprint with the same code the run uses, and the `value` must convert to the type the path holds. [Scenarios versus `LocalIngress`](connectors.md#scenarios-versus-localingress) compares the two. The [guide](../guides/yaml-blueprints.md#2-script-an-experiment) has a complete file.

A resource follows a capacity change within the same simulation instant, but after the step that made it. A telemetry sample taken at exactly that instant can therefore still show the old capacity.

---

## Referencing Python with `!python`

`!python module.attribute` imports `module` and returns its `attribute`. The reference can name anything: a function, a class, a dictionary or a constant. For example, `payload: !python workshop_logic.grade_part` makes a function the payload of a task, and `bootstrap_servers: !python kafka_logic.BOOTSTRAP_SERVERS` reads a constant that the module took from an environment variable.

**Resolution.** The longest prefix that is a module is imported, and the rest is read as attributes, so `pkg.module.func` and `module.Class.attribute` both work. The folder of the YAML file is put at the front of `sys.path` before anything is resolved, so a module beside the blueprint is found from any working directory. Other modules are found on the normal `sys.path`, which includes installed packages.

**Where it is accepted, and what is checked.** Every reference is resolved and checked when the file is loaded, so a wrong reference fails before the run starts, with the file and line.

| Field | Must be | Called as |
|---|---|---|
| `processes[].function` | a generator function | `function(context, **kwargs)` when the run starts |
| `tasks.<name>.payload` | a callable, or a mapping | `payload(task_id, context)` once per finished task |
| `telemetry[].function` | a callable | `function(context)` every `interval` |
| `egress[].when` | a callable | `when(record)`, True to send the record to that sink |
| `ingress[].type`, `egress[].type` | a class | `type(**config)` while the blueprint is built |
| `run.before[]` | a callable | `before()` once, at the start of `run()` |
| any value under `config`, `simulation` or `run.until` | whatever that argument takes | not called |

**Arguments.** A process is written either as a bare reference, `- !python module.func`, which is called as `func(context)`, or as a mapping with `function` and `kwargs`, which is called as `func(context, **kwargs)`. The [hybrid blueprint in the guide](../guides/yaml-blueprints.md#4-add-python-with-python) starts `maintenance(context, resource="drill", every=50, duration=5)` this way.

`kwargs` is what lets a blueprint start a process whose signature is not `(context)`. When the signature differs more, write a small adapter: a generator function that takes the context, picks out what the process needs, and hands over with `yield from`.

```python
def arrivals(context, product):
    yield from arrival_process(
        context.env, product, False, 5, context.sampler,
        context.get_resource(f"mill_{product}"),
    )
```

[Lifting an existing twin](../guides/yaml-blueprints.md#5-lift-an-existing-twin) shows a whole adapter for the OML hot rolling twin, whose processes take `(env, sampler, resource)`.

**A process must be a generator function**, one that contains `yield`. The check runs when the file is loaded and looks only at the function, so an adapter that returns a generator instead of yielding from one is rejected. Write adapters with `yield from`.

---

## Limitations

A blueprint covers configuration. Anything that is logic stays in Python.

- **No expression language for field values.** A mapping payload is a constant, apart from the task id that `id_field` adds. Weighted or Zipf choices, fields derived from other fields, values drawn from distributions, Markov walks and state carried from one event to the next are all Python, referenced with `!python`. The [Postgres](../examples/yaml/postgres.md) and [Redis](../examples/yaml/redis.md) examples are like this: their generators draw random values, so they are processes in Python.
- **Containers and stores exist only in the registry.** `containers` registers capacities, as `add_container` does, but no SimPy container is created for them. A process that needs one builds a `DynamicContainer` itself, or reads and writes the registry path directly, as the hot rolling twin does with its wear levels. `SimParameter.stores` has no builder method, so neither the builder nor a blueprint can register stores.
- **Built-in telemetry reads resources only.** Container levels, variables or derived metrics need a telemetry `function`.
- **A task needs a service and a resource.** Multi-stage routing, several resources per task, priorities and preemption are processes in Python.
- **One simulation per file.** A blueprint has one `sim_id`.
- **No environment variables in the file.** Read them in a module and reference the value, as `kafka_logic.BOOTSTRAP_SERVERS` does.
- **Only `!python` is added to YAML.** Every other tag, such as `!!python/object`, is rejected as `yaml.safe_load` rejects it.

---

## Errors

Every error names the file and the line, and `dynamic-des run` prints it after `Error:`. Validation errors are all reported at once:

```text
Error: err.yaml:3: simulation.speed: Extra inputs are not permitted
err.yaml:9: arrivals.standard.dist: Input should be 'exponential', 'normal' or 'lognormal'
```

`!python` references are resolved while the file is read, and cross-references are checked after validation: a task's `service` and `resource`, an arrival's `spawn`, each telemetry `publish` entry, and each scenario `path` and `value`.

```text
Error: err.yaml:6: !python workshop_logic.missing: 'workshop_logic' has no attribute 'missing'
Error: err.yaml:11: task 'part' uses service 'drilling', which is not defined under services
Error: err.yaml:6: 'Line_A.resources.press.current_cap' is not a registry path. Paths look like Line_A.resources.<name>.current_cap or Line_A.arrival.<name>.rate
```

---

## Security

**Loading a blueprint runs the imports it names.** `!python` imports modules, and importing a module runs its top-level code. A blueprint is therefore as trusted as a Python script: load only files you would run as code, in a local or trusted CI workflow. Every other node is constructed by `yaml.SafeLoader`, and the tag is registered on a subclass of it, so `yaml.safe_load` elsewhere in the process is unaffected.
