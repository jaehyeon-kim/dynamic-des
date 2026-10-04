# Advanced YAML: Custom Logic with `!python`

A blueprint holds configuration as plain YAML, and the [first guide](yaml-blueprints.md) covers that. When a simulation needs logic that YAML cannot express, the file can reference a Python object with `!python module.attribute` in place of a value. This guide covers when to use it, how a reference is found, what each field receives and the security note, and ends with the OML hot rolling twin as a worked case.

The [advanced Postgres example](../examples/advanced-postgres-orders.md) is a complete blueprint with one Python process. Every file on this page is in the repository under [`docs/snippets/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/docs/snippets).

---

## When to use it

Use `!python` when a blueprint needs one of these:

- **Field values with logic.** Random or weighted choices, fields derived from other fields and state carried from one event to the next are a payload function or a process.
- **More than one task on one resource.** Multi-stage routing, several resources per task, priorities and preemption are processes.
- **Telemetry other than resource statistics.** Container levels, variables and derived metrics are a telemetry function.
- **Routing over several destinations, and serializers.** A router or an Avro serializer is a Python object.
- **Setup a connector does not do itself**, such as topics that only a router knows about.
- **A connector from another package.**

A setting that is a value needs no Python. Environment variables, relative times, `when: history` and `when: live`, filesystem and catalog mappings, Iceberg schemas and Postgres `tables` are all plain YAML, as the [reference](../architecture/yaml.md) lists. A value can still come from a module when another program already defines it, as the hot rolling twin below reads its broker address from `src/config.py`.

---

## 1. How a reference is resolved

`!python module.attribute` imports `module` and returns its `attribute`. The reference can name anything: a function, a class, a dictionary or a constant.

The longest prefix that is a module is imported, and the rest is read as attributes, so `pkg.module.func` and `module.Class.attribute` both work. The folder of the YAML file is added to the front of `sys.path`, unless it is already on it, before anything is resolved, so a module beside the blueprint is found from any working directory. Other modules are found on the normal `sys.path`, which includes installed packages. `${VAR}` is replaced inside a reference too, so `!python ${LOGIC}.ticker` picks the module from the environment.

Every reference is resolved and checked when the file is loaded, so a wrong reference fails before the run starts, with the file and line.

| Field | Must be | Called as |
|---|---|---|
| `processes[].function` | a generator function | `function(context, **kwargs)` when the run starts |
| `tasks.<name>.payload` | a callable, or a mapping | `payload(task_id, context)` once per finished task |
| `telemetry[].function` | a callable | `function(context)` every `interval` |
| `egress[].when` | a callable, or `history` or `live` | `when(record)`, True to send the record to that sink |
| `ingress[].type`, `egress[].type` | a class, or a short name | `type(**config)` while the blueprint is built |
| `run.before[]` | a callable | `before()` once, at the start of `run()` |
| any value under `config`, `simulation` or `run.until` | whatever that argument takes | not called |

---

## 2. Processes, payloads and kwargs

This blueprint keeps the workshop in YAML and moves three pieces of logic into Python: a payload that grades each part, a maintenance process with arguments, and a predicate that keeps telemetry off the terminal:

```yaml title="docs/snippets/yaml/hybrid.yaml"
# The workshop with its logic in Python. YAML keeps the blueprint and the
# experiment; workshop_logic.py beside this file holds what YAML cannot express.
simulation:
  sim_id: Workshop
  factor: 0
  random_seed: 1

egress:
  - type: Console
    # Only events reach the terminal; telemetry is dropped.
    when: !python workshop_logic.events_only

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
    # Called as grade_part(task_id, context) for every finished part.
    payload: !python workshop_logic.grade_part

processes:
  # Called as maintenance(context, resource="drill", every=50, duration=5).
  - function: !python workshop_logic.maintenance
    kwargs: {resource: drill, every: 50, duration: 5}

scenario:
  - {at: 1 min, path: Workshop.arrival.parts.rate, value: 0.4}

run:
  until: 3 min
```

```python title="docs/snippets/yaml/workshop_logic.py"
"""Python for hybrid.yaml: a payload, a process with arguments and a predicate."""


def grade_part(task_id: int, context):
    """Grades each part from the shared random generator, so a seed repeats it."""
    grade = "A" if context.sampler.rng.random() < 0.9 else "B"
    return {"status": "finished", "part_id": task_id, "grade": grade}


def maintenance(context, resource: str, every: float, duration: float):
    """Waits `every` seconds, then takes the machine out of service for `duration`."""
    path = f"{context.sim_id}.resources.{resource}.current_cap"
    registry = context.env.registry
    while True:
        yield context.env.timeout(every)
        normal = registry.get(path).value
        registry.update(path, 0)
        context.env.publish_event(f"{resource}-maintenance", {"status": "down"})
        yield context.env.timeout(duration)
        registry.update(path, normal)
        context.env.publish_event(f"{resource}-maintenance", {"status": "up"})


def events_only(record: dict) -> bool:
    """Egress predicate: keeps events and drops telemetry."""
    return record["stream_type"] == "event"
```

What each reference receives:

- `payload` is called as `grade_part(task_id, context)` for every finished part. It draws from `context.sampler`, the generator seeded by `random_seed`, so the grades repeat with the seed.
- `processes` entries are called as `function(context, **kwargs)` when the run starts. `kwargs` is how one function serves several machines.
- `when` is called with every record and returns True to send it to that sink.

The run shows both:

```text
[EVT] {'sim_ts': 10.271, 'timestamp': '...', 'key': 'task-0', 'value': {'status': 'finished', 'part_id': 0, 'grade': 'B'}}
[EVT] {'sim_ts': 50.0, 'timestamp': '...', 'key': 'drill-maintenance', 'value': {'status': 'down'}}
[EVT] {'sim_ts': 55.0, 'timestamp': '...', 'key': 'drill-maintenance', 'value': {'status': 'up'}}
```

A process is written either as a bare reference, `- !python module.func`, which is called as `func(context)`, or as a mapping with `function` and `kwargs`, which is called as `func(context, **kwargs)`. `kwargs` is what lets a blueprint start a process whose signature is not `(context)`. When the signature differs more, write a small adapter: a generator function that takes the context, picks out what the process needs, and hands over with `yield from`.

```python
def arrivals(context, product):
    yield from arrival_process(
        context.env, product, False, 5, context.sampler,
        context.get_resource(f"mill_{product}"),
    )
```

**A process must be a generator function**, one that contains `yield`. The check runs when the file is loaded and looks only at the function, so an adapter that returns a generator instead of yielding from one is rejected. Write adapters with `yield from`.

---

## 3. Routers, serializers and connector classes

A router replaces the default destination of every record. `path_router` on Parquet and JSONL and `table_router` on Iceberg each take a function that receives a record and returns its destination, or None to drop it. `topic_router` on Kafka must always return a topic. To drop records before Kafka, give the sink a `when` predicate. With a router, records are written as the router leaves them, so flattening events and dropping telemetry become the router's job. Kafka also leaves topic creation to the caller, because only the router knows which topics it uses. The hot rolling twin below sends every record to one of four topics with `topic_router: !python routing.custom_topic_router`.

A serializer is an object, so it is built in a module and referenced. `KafkaEgress` takes `default_serializer` and `topic_serializers`, for example `default_serializer: !python serializers.AVRO`, where `serializers.py` builds a `ConfluentAvroSerializer` with the registry URL and the schema.

A connector from another package is referenced by class: `type: !python my_package.MyEgress`. Its `config` is passed to the constructor, as for a built-in one.

---

## 4. Setup with `run.before`

`run.before` lists functions called with no arguments, once, at the start of `run()`. Connectors already create what they write to when they can: Kafka its event and telemetry topics, Postgres the tables under `tables`, and Parquet and JSONL their folders. `run.before` is for anything else, such as the five topics the hot rolling twin below creates for its router.

Put such setup in a function rather than at the top of the module. The module is imported when the file is loaded, so its top-level code runs even when the file is loaded and never run.

---

## 5. Worked case: lift the OML hot rolling twin

The [OML hot rolling digital twin](https://github.com/jaehyeon-kim/oml-digital-twin-hotrolling) is a steel mill written with the low-level API. Its `generator.py` builds a `SimParameter` with three arrivals, three services, three resources, three containers and three variables, wires Kafka in both directions with a topic router, and starts the processes in `sim_logic.py`. Those processes take `(env, sampler, resource, ...)`, not a context, and `sim_logic.py` runs to 262 lines of multi-stage routing, wear and physics.

The configuration moves to YAML. The processes stay as they are, and a small adapter changes only how they are called.

```yaml title="docs/snippets/hot_rolling/hot_rolling.yaml"
# The OML hot rolling digital twin as a blueprint. The parameters and the Kafka
# wiring that generator.py builds in Python are declared here. The processes stay
# in sim_logic.py, and hot_rolling_adapter.py beside this file adapts them.
#
# Place this file and the adapter in sim_control/, then run from there:
#   ddes run hot_rolling.yaml
simulation:
  sim_id: HotRolling
  factor: 1.0
  random_seed: 42

ingress:
  # Control commands from the dashboard.
  - type: Kafka
    config:
      bootstrap_servers: !python src.config.KAFKA_BROKER
      topic: !python src.config.TOPIC_CONTROL_INGRESS

egress:
  # One producer; the router picks one of four topics for every record.
  - type: Kafka
    config:
      bootstrap_servers: !python src.config.KAFKA_BROKER
      topic_router: !python routing.custom_topic_router

arrivals:
  structural: {dist: exponential, rate: 0.2}
  microalloyed: {dist: exponential, rate: 0.15}
  high_alloy: {dist: exponential, rate: 0.1}

services:
  pass_roughing: {dist: normal, mean: 2.0, std: 0.2}
  pass_intermediate: {dist: normal, mean: 4.5, std: 0.5}
  pass_finishing: {dist: normal, mean: 8.0, std: 1.2}

resources:
  mill_structural: {current_cap: 4, max_cap: 10}
  mill_microalloyed: {current_cap: 4, max_cap: 10}
  mill_high_alloy: {current_cap: 4, max_cap: 10}

containers:
  wear_structural: {current_cap: 0.001, max_cap: 100.0}
  wear_microalloyed: {current_cap: 0.001, max_cap: 100.0}
  wear_high_alloy: {current_cap: 0.001, max_cap: 100.0}

variables:
  velocity_structural: {type: abrupt, value: 0.0}
  velocity_microalloyed: {type: abrupt, value: 0.0}
  velocity_high_alloy: {type: abrupt, value: 0.0}

processes:
  # Started in the order generator.py starts them.
  - function: !python hot_rolling_adapter.drift
    kwargs: {product_lines: [structural, microalloyed, high_alloy]}
  - function: !python hot_rolling_adapter.monitor
    kwargs: {product_lines: [structural, microalloyed, high_alloy]}
  - function: !python hot_rolling_adapter.primer
    kwargs: {product: structural}
  - function: !python hot_rolling_adapter.arrivals
    kwargs: {product: structural}
  - function: !python hot_rolling_adapter.primer
    kwargs: {product: microalloyed}
  - function: !python hot_rolling_adapter.arrivals
    kwargs: {product: microalloyed}
  - function: !python hot_rolling_adapter.primer
    kwargs: {product: high_alloy}
  - function: !python hot_rolling_adapter.arrivals
    kwargs: {product: high_alloy}

# A reproducible drift experiment, which the dashboard otherwise drives by hand:
# gradual wear on the structural line from 10 minutes.
scenario:
  - at: 10 min
    path: HotRolling.variables.velocity_structural
    value: {type: gradual, value: 0.5, freq: 1}

run:
  before:
    - !python hot_rolling_adapter.create_topics
```

```python title="docs/snippets/hot_rolling/hot_rolling_adapter.py"
"""Adapts the hot rolling processes to the calls a blueprint makes.

A blueprint calls every process as `function(context, **kwargs)`. The processes in
sim_logic.py take `(env, sampler, resource, ...)` instead, so each function here
takes the context, picks out what the process needs, and hands over with
`yield from`. Nothing in sim_logic.py changes.
"""

import time
import uuid

from dynamic_des import KafkaAdminConnector
from generator import telemetry_monitor
from sim_logic import arrival_process, drift_engine, roll_slab
from src.config import (
    KAFKA_BROKER,
    TOPIC_CONTROL_INGRESS,
    TOPIC_GROUND_TRUTH,
    TOPIC_LIFECYCLE,
    TOPIC_PREDICTION_REQUESTS,
    TOPIC_TELEMETRY,
)

# The command-line options of generator.py, fixed here.
VARIABLE_PASSES = False
MAX_PASSES = 5


def drift(context, product_lines):
    yield from drift_engine(context.env, product_lines)


def monitor(context, product_lines):
    yield from telemetry_monitor(context.env, product_lines)


def primer(context, product):
    """Rolls a first slab, so the mill does not start dry."""
    yield from roll_slab(
        context.env,
        uuid.uuid4().hex[:8].upper(),
        product,
        VARIABLE_PASSES,
        MAX_PASSES,
        context.sampler,
        context.get_resource(f"mill_{product}"),
    )


def arrivals(context, product):
    yield from arrival_process(
        context.env,
        product,
        VARIABLE_PASSES,
        MAX_PASSES,
        context.sampler,
        context.get_resource(f"mill_{product}"),
    )


def create_topics():
    """Creates the five topics generator.py creates before it starts."""
    KafkaAdminConnector(bootstrap_servers=KAFKA_BROKER).create_topics(
        topics_config=[
            {"name": TOPIC_CONTROL_INGRESS, "partitions": 1},
            {"name": TOPIC_TELEMETRY, "partitions": 1},
            {"name": TOPIC_LIFECYCLE, "partitions": 1},
            {"name": TOPIC_PREDICTION_REQUESTS, "partitions": 3},
            {"name": TOPIC_GROUND_TRUTH, "partitions": 3},
        ]
    )
    time.sleep(2)
```

**What moved.** The whole `SimParameter`, both Kafka connectors with the custom topic router, the seed, the factor and the topic creation. The broker address and the topic names stay in `src/config.py`, and the blueprint references them. `containers` and `variables` carry the wear levels and drift velocities the processes read and write.

**What stayed.** `drift_engine`, `arrival_process`, `roll_slab` and `telemetry_monitor`, unchanged. The adapter is one generator function per process. Each takes the context, picks out the environment, the sampler and the mill resource, and hands over with `yield from`. The primer slab and the arrival loop are separate entries, so they start in the order `generator.py` starts them.

**What is new.** The scenario turns on gradual wear on the structural line at ten minutes. The twin's dashboard does this by hand over Kafka. As a scenario it repeats exactly, and it can still be combined with the dashboard, because both write to the same registry.

Built with the same seed at `factor: 0`, this blueprint and `generator.py` produced the same 3,830 records in the first 300 simulation seconds, in the same order, once the random slab ids from `uuid4` were masked. The registry differs in one way that does not change the run: the builder stores `mean: 0.0` and `std: 0.0` for an exponential arrival where the hand-built `SimParameter` stores `None`, and an exponential distribution reads neither.

---

## 6. Errors

A reference that cannot be resolved stops the load with its line:

```text
Error: err.yaml:6: !python workshop_logic.missing: 'workshop_logic' has no attribute 'missing'
```

A module that exists but fails on one of its own imports is reported with that error, rather than as a missing module.

---

## 7. Security

**Loading a blueprint runs the imports it names.** `!python` imports modules, and importing a module runs its top-level code. A blueprint is therefore as trusted as a Python script: load only files you would run as code, in a local or trusted CI workflow.

Every other node is constructed by `yaml.SafeLoader`, and the tag is registered on a subclass of it, so `yaml.safe_load` elsewhere in the process is unaffected. Only `!python` is added to YAML. Every other tag, such as `!!python/object`, is rejected as `yaml.safe_load` rejects it.
