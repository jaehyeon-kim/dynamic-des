# Low-Level API

The low-level API is `DynamicRealtimeEnvironment` used directly. A script registers the parameters, attaches the connectors, creates the resources and starts SimPy processes with `env.process`. The [declarative API](context.md) does each of these steps inside `run()`, so this page is also a description of what the builder does for you.

[Part 1 of the tutorials](../tutorials/low-level.md) builds a first factory this way.

---

## Example

The Low-Level API exposes `DynamicRealtimeEnvironment` directly. You are responsible for instantiating and configuring the registry, setting up I/O connectors manually, and writing raw SimPy generators.

Use it when you need to bypass the standard telemetry rules, or to create new topics and environments while the simulation runs.

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

## Steps of a Run

A low-level script takes these steps, in this order:

1. **Create the environment**: `DynamicRealtimeEnvironment(factor=1.0, logical_start_time=None, go_live_at=None)`. [Time](time.md) explains the three arguments.
2. **Register the parameters**: `env.registry.register_sim_parameter(SimParameter(...))`. Every path in [Registry and Live Parameters](registry.md) comes from this call.
3. **Attach the connectors**: `env.setup_ingress([...])` starts the ingress thread, and `env.setup_egress([...])` starts the egress thread. [Batching and Delivery](batching.md) lists the arguments of `setup_egress`.
4. **Create the resources**: `DynamicResource(env, sim_id, name)`. A resource reads its capacity from the registry when it is created, so a resource whose paths are not registered raises `KeyError`.
5. **Start the processes**: `env.process(generator)` for each SimPy generator.
6. **Run, then tear down**: `env.run(until=...)` inside `try`, and `env.teardown()` in `finally`. Teardown flushes the egress buffers and stops both threads.

`env.run` is SimPy's, so `until` is a number of simulation seconds. Convert a duration such as `"1 week"` with `time_to_seconds`, which `dynamic_des` exports.

---

## What the Builder Adds

`SimulationContext.run()` takes the same steps, and adds these:

* **A seeded sampler.** The builder creates `Sampler(rng=np.random.default_rng(random_seed))`. A low-level script creates its own. A `Sampler` with no `rng` draws nothing at random: it returns the mean of each distribution, or `1 / rate` for an exponential one, every time.
* **One resource per `add_resource`.** The builder creates a `DynamicResource` for each, and `context.get_resource(name)` returns it.
* **Task lifecycle events.** `@app.task` publishes `queued` and `started` events and samples the service time. A low-level process publishes its own events with `env.publish_event(key, value)`.
* **Metric names with the `sim_id`.** `context.publish(name, value)` prefixes the name with the `sim_id`. `env.publish_telemetry(path_id, value)` publishes the path exactly as given.

`publish_event` and `publish_telemetry` do nothing when no egress is attached, so a script without `setup_egress` runs without error and publishes nothing.

---

## Live Parameters in a Process

`env.registry.get_config(path)` returns the configuration object registered at that path, such as the `DistributionConfig` of `Line_A.service.milling`. An update to one of its paths changes that object in place, so a process that keeps the object and samples it again picks up the new value at its next sample:

```python
arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
while True:
    yield env.timeout(sampler.sample(arrival_cfg))
```

`env.registry.get(path).value` reads a single value, such as a variable. [Registry and Live Parameters](registry.md) describes how updates arrive.

---

## Stores

`SimParameter(stores=...)` registers stores, and only the low-level API can do so. [Resources and Containers](resources.md#creating-them) shows how to create a container or a store from its registered paths.
