# Local Simulation (YAML)

This example builds the same simulation as the [declarative local example](../declarative/local.md), from a YAML blueprint and nothing else. No Python is written: the arrival loop, the task and the telemetry loop are all declared in the file.

It writes to `ConsoleEgress` and needs no container, so it is the place to start.

---

## Quick Start

Download the blueprint, then run it.

```bash
curl -O https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/examples/yaml/local.yaml
```

### With uv

```bash
# 1. Run the blueprint
uv run --no-project --with dynamic-des ddes run local.yaml
```

### With pip

```bash
# 1. Install the package
pip install dynamic-des

# 2. Run the blueprint
ddes run local.yaml
```

## What It Does

The run prints every record to the terminal and stops after 60 simulation seconds, which at `factor: 1.0` is one real minute. Telemetry lines carry `[TEL]` and events carry `[EVT]`:

```text
[TEL] {'sim_ts': 0.0, 'timestamp': '...', 'path_id': 'Factory_A.utilization', 'value': 0.0}
[EVT] {'sim_ts': 0.328, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Factory_A.service.milling', 'status': 'queued'}}
[EVT] {'sim_ts': 3.754, 'timestamp': '...', 'key': 'task-0', 'value': {'event_type': 'part_produced', 'quality': 'A', 'part_id': 0}}
```

Each part produces a `queued`, a `started` and a finished event. The finished event is the task's `payload`, with the task id added as `part_id` because of `id_field`. Add `--until 10` to stop after 10 simulation seconds instead.

## Full Source Code

The blueprint declares one resource, one service and one arrival. `spawn` names the task each arrival starts, and the `telemetry` entry publishes two statistics of the lathe every 2 simulation seconds.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

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
