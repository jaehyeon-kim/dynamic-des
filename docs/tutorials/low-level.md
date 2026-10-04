# Part 1: Low-level API

This tutorial builds a small factory directly on `DynamicRealtimeEnvironment`: one lathe, parts arriving about every 2 seconds, and 1.5 seconds of machining per part. Every step is written by hand, with plain SimPy processes started by `env.process`. [Part 2](01-first-factory.md) builds the same factory with the declarative API, which does these steps for you, and [Part 3](yaml.md) builds it as a YAML blueprint.

---

## 1. Setup and imports

Install the core package:

```bash
pip install dynamic-des
```

Create a file called `first_factory_low_level.py` and add the imports:

```python
import logging

import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
```

---

## 2. Describe the parameters

A `SimParameter` holds every parameter of one simulation, under one `sim_id`. This one has an arrival stream named `parts`, a service named `machining` and a resource named `lathe` with a capacity of 1. With no `std`, a normal distribution returns its mean every time.

```python
PARAMS = SimParameter(
    sim_id="Factory_A",
    arrival={"parts": DistributionConfig(dist="exponential", rate=0.5)},
    service={"machining": DistributionConfig(dist="normal", mean=1.5)},
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=1)},
)
```

When the parameters are registered, each value gets a path in the registry, such as `Factory_A.arrival.parts.rate` or `Factory_A.resources.lathe.current_cap`. An ingress connector changes a value through its path while the run continues. [Registry and Live Parameters](../architecture/registry.md) lists every path.

---

## 3. Write the part process

A process is a SimPy generator. This one publishes a `queued` event, waits for the lathe, publishes `started`, waits for a machining time drawn from the `machining` distribution, and publishes the finished event. Leaving the `with` block releases the lathe.

```python
def process_part(env, sampler, lathe, part_id):
    """One part: wait for the lathe, machine it, release it."""
    key = f"task-{part_id}"
    path_id = "Factory_A.service.machining"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})

    with lathe.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        service = env.registry.get_config(path_id)
        yield env.timeout(sampler.sample(service))
        env.publish_event(key, {"part_id": part_id})
```

`env.registry.get_config` returns the live `DistributionConfig` of the service. The time is drawn after the lathe is acquired, so a change to the service made while the part waits in the queue still applies to it.

---

## 4. Write the arrival loop

The arrival loop waits for a gap drawn from the `parts` distribution, then starts a new part process with `env.process`:

```python
def parts_generator(env, sampler, lathe):
    """Starts one process_part for every arrival."""
    arrival = env.registry.get_config("Factory_A.arrival.parts")
    part_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(process_part(env, sampler, lathe, part_id))
        part_id += 1
```

---

## 5. Wire up the environment and run

`run` creates the environment and takes the steps in order:

1. Register the parameters. A resource reads its capacity from the registry, so this comes first.
2. Attach `ConsoleEgress`, which prints every record. Without an egress, published events go nowhere.
3. Create the lathe and a `Sampler`. The sampler draws from a NumPy generator. Pass a seed, such as `np.random.default_rng(42)`, to repeat the same run.
4. Start the arrival loop, and run until simulation time `until`.
5. Tear down in `finally`, which flushes the last records and stops the background thread.

```python
def run(until=10.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])

    lathe = DynamicResource(env, "Factory_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng())

    env.process(parts_generator(env, sampler, lathe))
    try:
        env.run(until=until)
    finally:
        env.teardown()
```

At `factor=1.0` one simulated second takes one real second, so the run takes 10 seconds.

```python
if __name__ == "__main__":
    print("Starting simulation...")
    run()
```

---

## 6. Run it

```bash
python first_factory_low_level.py
```

Each record is printed as it is written. `[EVT]` lines are events and `[TEL]` lines are telemetry:

```text
[TEL] {'sim_ts': 0.0, 'timestamp': '...', 'path_id': 'system.simulation.lag_seconds', 'value': 0.0}
[EVT] {'sim_ts': 0.171, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Factory_A.service.machining', 'status': 'queued'}}
[EVT] {'sim_ts': 0.171, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Factory_A.service.machining', 'status': 'started'}}
[EVT] {'sim_ts': 1.671, 'timestamp': '...', 'key': 'task-0', 'value': {'part_id': 0}}
```

The only telemetry is `system.simulation.lag_seconds`, which every run with an egress publishes once per simulation second. It shows how far the simulation is behind the wall clock. The arrival times are random, so each run shows different values.

---

## Full script

The complete file, which the documentation tests run:

```python title="docs/snippets/tutorials/first_factory_low_level.py"
"""Part 1 of the tutorials: the first factory, on the low-level API.

One lathe, parts arriving about every 2 seconds, and 1.5 seconds of machining per
part. Every record is printed to the terminal.
"""

import logging

import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Factory_A",
    arrival={"parts": DistributionConfig(dist="exponential", rate=0.5)},
    service={"machining": DistributionConfig(dist="normal", mean=1.5)},
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=1)},
)


def process_part(env, sampler, lathe, part_id):
    """One part: wait for the lathe, machine it, release it."""
    key = f"task-{part_id}"
    path_id = "Factory_A.service.machining"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})

    with lathe.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        service = env.registry.get_config(path_id)
        yield env.timeout(sampler.sample(service))
        env.publish_event(key, {"part_id": part_id})


def parts_generator(env, sampler, lathe):
    """Starts one process_part for every arrival."""
    arrival = env.registry.get_config("Factory_A.arrival.parts")
    part_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(process_part(env, sampler, lathe, part_id))
        part_id += 1


def run(until=10.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])

    lathe = DynamicResource(env, "Factory_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng())

    env.process(parts_generator(env, sampler, lathe))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    print("Starting simulation...")
    run()
```

[Part 2](01-first-factory.md) builds the same factory with the declarative API.
