# Standard, Low-Level and YAML Paradigms

Dynamic DES provides three ways to build your event-driven simulations, allowing you to choose between ease of use and raw control. A YAML blueprint holds only configuration, the Standard API adds Python logic through a builder, and the Low-Level API gives raw control.

---

## Three Paradigms

| Feature | YAML Blueprint | Standard API (Declarative) | Low-Level API (Imperative) |
|---|---|---|---|
| **Entry Point** | `ddes run` or `SimulationContext.from_yaml` | `SimulationContext` | `DynamicRealtimeEnvironment` |
| **Philosophy** | Declare the configuration in a file, and reference Python for the logic. | Define *what* the system looks like and use decorators for task lifecycles. | Define *how* every event and resource operates step-by-step. |
| **Boilerplate** | None for configuration. Logic is Python referenced with `!python`. | Low (Automatic event emission, resource requesting, and sampling). | High (Manual queueing, starting, timing out, and releasing). |
| **Typical Use Case** | Varying parameters, connectors and timed experiments between runs without editing code. | Building standard digital twins, historical data generation, and forecasting pipelines. | Edge-case scenarios requiring dynamic topology changes mid-run. |

The three are layers, not alternatives. A blueprint is built through the Standard API's builder methods, and the builder runs on the Low-Level API, so a blueprint can reference Python written for the builder, and a builder process can use the environment directly.

### Which to choose

* **YAML Blueprint** when the configuration is what changes between runs: rates, capacities, connectors, batching or a scripted experiment. The file is easy to review and diff, and it has a built-in scenario of timed changes on the simulation clock. Logic stays in a Python module beside the file.
* **Standard API** when the logic is most of the program and you want it in one Python file, or when you build the configuration in code, for example from a loop.
* **Low-Level API** when you need what the builder does not do, such as resources created mid-run or full control of every event.

---

## 1. Standard API (Declarative)
The Standard API uses the `SimulationContext` builder to configure the twin's resources, distributions, ingress/egress parameters, and I/O connectors.

All execution logic is declared using clean Python decorators:

```python
from dynamic_des import SimulationContext, ConsoleEgress

app = (
    SimulationContext(sim_id="Line_A", factor=1.0)
    .add_resource("lathe", current_cap=2, max_cap=5)
    .add_arrival("standard", dist="exponential", rate=1.0)
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    .add_egress(ConsoleEgress())
)

@app.arrival_loop("standard")
def generate(context: SimulationContext):
    task_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(run_task(task_id))
        task_id += 1

@app.task(service_id="milling", resource_id="lathe")
def run_task(task_id: int):
    # This function is automatically wrapped with:
    # 1. Emission of a "queued" event to Kafka/Console.
    # 2. Block until the "lathe" resource is acquired.
    # 3. Emission of a "started" event.
    # 4. Yield of the "milling" timeout (sampled from the distribution).
    # 5. Emission of a "finished" event with the dictionary returned below.
    return {"part_id": task_id}
```

---

## 2. Low-Level API (Imperative)
The Low-Level API exposes `DynamicRealtimeEnvironment` directly. You are responsible for instantiating and configuring the registry, setting up I/O connectors manually, and writing raw SimPy generators.

This is ideal when you need to bypass standard telemetry rules or dynamically construct new topics and environments on the fly.

```python
import numpy as np
from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

env = DynamicRealtimeEnvironment(factor=1.0)
# A resource reads its capacity from the registry, so register it first
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Line_A",
        resources={"lathe": CapacityConfig(current_cap=1, max_cap=1)},
    )
)
egress = ConsoleEgress()
env.setup_egress([egress])

res = DynamicResource(env, "Line_A", "lathe")
sampler = Sampler(rng=np.random.default_rng(42))

def manual_generator(env, res):
    task_id = 0
    while True:
        # Manual arrival sampling
        yield env.timeout(1.0)

        # Manual event emission
        task_key = f"task-{task_id}"
        env.publish_event(task_key, {"status": "queued"})

        # Manual resource requesting
        with res.request() as req:
            yield req
            env.publish_event(task_key, {"status": "started"})
            yield env.timeout(3.0)  # Manual service duration
            env.publish_event(task_key, {"status": "finished"})

        task_id += 1
```

---

## 3. YAML Blueprint
A blueprint declares the same configuration as the builder chain in a YAML file, and `ddes run` builds and runs it. Simple tasks, arrival loops and resource telemetry are declared in the file. Processes, payload functions and routers are Python, referenced with `!python`. The [local example](../examples/yaml/local.md) needs no Python at all, and the [YAML Blueprints reference](yaml.md) covers every section.

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
