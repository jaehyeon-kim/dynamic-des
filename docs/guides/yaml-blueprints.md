# YAML Blueprints, from First File to Hybrid

A blueprint puts the configuration of a simulation in a YAML file: the machines, the distributions, the connectors and the experiment. The logic stays in Python. This guide starts with a file that needs no Python at all, then adds a scripted experiment, connectors, Python functions referenced with `!python`, and finally lifts an existing digital twin whose processes cannot change.

The [YAML Blueprints reference](../architecture/yaml.md) lists every section and field. Every file on this page is in the repository under [`docs/snippets/`](https://github.com/jaehyeon-kim/dynamic-des/tree/main/docs/snippets), and the tests build each one.

---

## When to use a blueprint

Use a blueprint when the configuration is what changes between runs: a new arrival rate, a second machine, a different broker, a capacity experiment. A YAML file is easier to review and to diff than a builder chain, and a run is one command.

Stay in Python when most of the program is logic. Of the 13 programs surveyed in issue #13, only one needed no Python at all. In practice a blueprint holds the configuration and a module beside it holds the logic, and `!python` joins the two.

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
# The twin of examples/declarative/redis_example.py. Factory writes every record to
# the events Redis Stream through RedisEgress, while RedisIngress subscribes to the
# simulation_params channel. The part generator stays in redis_logic.py beside
# this file.
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
  part_arrival: {dist: exponential, rate: 2.0}

processes:
  # Publishes one part event on every part_arrival.
  - !python redis_logic.part_generator
```

The other options of `add_egress` sit beside `config`: `when` routes records, and `batch_size` and `flush_interval` give one sink its own cadence. `batching` sets the defaults, as `with_batching` does. The [YAML examples](../examples/yaml/local.md) cover every connector the declarative examples use: Console, Kafka, Parquet, Iceberg, Postgres and Redis, and the backfill-then-live run that sends history to Parquet and the live tail to Kafka.

A connector from another package is referenced by class: `type: !python my_package.MyEgress`. Its `config` is passed to the constructor, as for a built-in one.

---

## 4. Add Python with `!python`

The Redis example already uses Python: its generator draws a random part type for every arrival, which a blueprint has no syntax for. `!python module.attribute` imports the module and uses the attribute in place of a value. The folder of the YAML file is searched first, so a module beside it is found from any working directory.

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

Every reference is resolved and checked when the file is loaded. A process must be a generator function, a payload, predicate or telemetry function must be callable, and a connector `type` must be a class. A wrong reference stops the load with its line.

**Loading a blueprint runs the imports it names**, and importing a module runs its top-level code. Treat a blueprint as you would a Python script, and load only files you trust.

---

## 5. Lift an existing twin

The [OML hot rolling digital twin](https://github.com/jaehyeon-kim/oml-digital-twin-hotrolling) is a steel mill written with the low-level API. Its `generator.py` builds a `SimParameter` with three arrivals, three services, three resources, three containers and three variables, wires Kafka in both directions with a topic router, and starts the processes in `sim_logic.py`. Those processes take `(env, sampler, resource, ...)`, not a context, and `sim_logic.py` runs to 262 lines of multi-stage routing, wear and physics.

The configuration moves to YAML. The processes stay as they are, and a small adapter changes only how they are called.

```yaml title="docs/snippets/hot_rolling/hot_rolling.yaml"
# The OML hot rolling digital twin as a blueprint. The parameters and the Kafka
# wiring that generator.py builds in Python are declared here. The processes stay
# in sim_logic.py, and hot_rolling_adapter.py beside this file adapts them.
#
# Place this file and the adapter in sim_control/, then run from there:
#   dynamic-des run hot_rolling.yaml
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

## 6. Limitations

A blueprint is configuration. These stay in Python:

- **Field values with logic.** A mapping payload is a constant, apart from the task id that `id_field` adds. Random or weighted choices, derived fields and state carried between events are a `!python` payload or process.
- **Containers and stores as SimPy objects.** `containers` registers capacities in the registry only, as `add_container` does. A process that needs a SimPy container builds a `DynamicContainer`, or reads and writes the registry path as the hot rolling twin does. Stores have no builder method, so neither API registers them.
- **Anything beyond one task on one resource.** Multi-stage routing, several resources per task, priorities and preemption are processes.
- **Telemetry other than resource statistics.** Container levels, variables and derived metrics need a telemetry `function`.
- **Environment variables.** The file has no interpolation. Read the variable in a module and reference the value, as `kafka_logic.BOOTSTRAP_SERVERS` does.

The [reference](../architecture/yaml.md#limitations) lists the full set.
