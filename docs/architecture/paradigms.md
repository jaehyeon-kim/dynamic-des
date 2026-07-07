# Standard vs. Low-Level Paradigms

Dynamic DES provides two ways to build your event-driven simulations, allowing you to choose between ease of use and raw control.

---

## Two Paradigms

| Feature | Standard API (Declarative) | Low-Level API (Imperative) |
|---|---|---|
| **Entry Point** | `SimulationContext` | `DynamicRealtimeEnvironment` |
| **Philosophy** | Define *what* the system looks like and use decorators for task lifecycles. | Define *how* every event and resource operates step-by-step. |
| **Boilerplate** | Low (Automatic event emission, resource requesting, and sampling). | High (Manual queueing, starting, timing out, and releasing). |
| **Typical Use Case** | Building standard digital twins, historical data generation, and forecasting pipelines. | Edge-case scenarios requiring dynamic topology changes mid-run. |

---

## 1. Standard API (Declarative)
The Standard API uses the `SimulationContext` builder to configure the twin's resources, distributions, ingress/ingress parameters, and I/O connectors.

All execution logic is declared using clean Python decorators:

```python
from dynamic_des import SimulationContext, ConsoleEgress

app = (
    SimulationContext(sim_id="Line_A", factor=1.0)
    .add_resource("lathe", current_cap=2)
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
from dynamic_des import DynamicRealtimeEnvironment, DynamicResource, Sampler, ConsoleEgress

env = DynamicRealtimeEnvironment(factor=1.0)
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
