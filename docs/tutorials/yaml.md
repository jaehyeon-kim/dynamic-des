# Part 3: YAML

This tutorial writes the factory from [Part 1](low-level.md) and [Part 2](01-first-factory.md) as a YAML blueprint, and runs it with the `ddes` command. The file holds only configuration, and no Python is written.

---

## 1. Write the blueprint

Install the core package, which also installs the `ddes` command:

```bash
pip install dynamic-des
```

Create a file called `first_factory.yaml`:

```yaml title="docs/snippets/yaml/first_factory.yaml"
# Part 3 of the tutorials: the first factory as a blueprint.
# Run it with: ddes run first_factory.yaml
simulation:
  sim_id: Factory_A
  factor: 1.0

egress:
  - type: Console

resources:
  lathe: {current_cap: 1, max_cap: 1}

services:
  # With no std, a normal distribution returns its mean every time.
  machining: {dist: normal, mean: 1.5}

arrivals:
  # One part every 2 seconds on average. Each arrival spawns one process_part.
  parts: {dist: exponential, rate: 0.5, spawn: process_part}

tasks:
  process_part:
    service: machining
    resource: lathe
    # The finished event's value. id_field adds the task id as part_id.
    payload: {}
    id_field: part_id

run:
  until: 10
```

Each section is one builder call from Part 2:

| Section | Part 2 | Part 1 |
|---|---|---|
| `simulation` | `SimulationContext(sim_id="Factory_A", factor=1.0)` | `DynamicRealtimeEnvironment(factor=1.0)` and the `sim_id` of `SimParameter` |
| `egress` | `.add_egress(ConsoleEgress())` | `env.setup_egress([ConsoleEgress()])` |
| `resources`, `services`, `arrivals` | `.add_resource`, `.add_service`, `.add_arrival` | the `SimParameter` fields |
| `spawn` on the arrival | the `@app.arrival_loop` function | `parts_generator` |
| `tasks` | the `@app.task` function | `process_part` |
| `run.until` | `app.run(until=10.0)` | `run(until=10.0)` |

`payload` is the value of the finished event. It is empty here, and `id_field` adds the task id to it as `part_id`, so the finished event is `{'part_id': 0}`, as in Part 2.

---

## 2. Run it

```bash
ddes run first_factory.yaml
```

The run prints the same records as Parts 1 and 2, and stops after 10 simulation seconds:

```text
[TEL] {'sim_ts': 0.0, 'timestamp': '...', 'path_id': 'system.simulation.lag_seconds', 'value': 0.0}
[EVT] {'sim_ts': 1.596, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Factory_A.service.machining', 'status': 'queued'}}
[EVT] {'sim_ts': 1.596, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Factory_A.service.machining', 'status': 'started'}}
[EVT] {'sim_ts': 3.096, 'timestamp': '...', 'key': 'task-0', 'value': {'part_id': 0}}
```

`--until` overrides `run.until`, so `ddes run first_factory.yaml --until 30` runs for 30 simulation seconds. A mistake in the file stops the load with the file and the line, before anything runs.

From Python, `SimulationContext.from_yaml("first_factory.yaml")` returns the same context that Part 2 builds, and `run()` runs it.

---

## Next steps

* [YAML Blueprints, from First File to Connectors](../guides/yaml-blueprints.md) adds a scripted experiment, connectors and settings per environment.
* [YAML Blueprints](../architecture/yaml.md) lists every section and field.
* The [examples](../examples/local.md) show each connector in YAML, beside the same example in Python.
