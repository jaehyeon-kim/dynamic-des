# Part 2: Declarative API

Part 2 of the tutorials uses the declarative API. It builds the factory [Part 1](low-level.md) built by hand, and [Part 3](yaml.md) writes it as a YAML blueprint. It has three steps: a first factory that runs locally, randomness and scheduled capacity changes, and Kafka connectors.

---

## 1. Your First Factory (Local)

In this first step, you will learn the fundamental lifecycle of a digital twin by building a simple factory model that runs locally on your machine.

### 1.1 Setup and imports

First, make sure you have the core `dynamic-des` package installed:
```bash
pip install dynamic-des
```

Create a new file called `first_factory.py` and add the imports:
```python
import logging
from dynamic_des import SimulationContext, ConsoleEgress

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s")
```

### 1.2 Initialize the Simulation Context

We will use the **Standard API (`SimulationContext`)** builder to wire up the simulation:
* We define a unique namespace prefix (`sim_id="Factory_A"`).
* We register a resource representing a single machine (`lathe`) with a capacity of 1.
* We configure a static arrival stream named `parts` where a new part arrives on average every 2 seconds.
* We register a service named `machining` that takes 1.5 seconds per part. With no `std`, a normal distribution returns its mean every time.
* We direct all simulation events to print directly to the console (`ConsoleEgress`).

```python
app = (
    SimulationContext(sim_id="Factory_A", factor=1.0)
    .add_resource("lathe", current_cap=1, max_cap=1)
    .add_arrival("parts", dist="exponential", rate=0.5) # 1 part every 2 seconds on average
    .add_service("machining", dist="normal", mean=1.5)
    .add_egress(ConsoleEgress())
)
```

### 1.3 Define Simulation Logic

Now, we use decorators to specify how tasks behave.

First, define the **arrival loop** that listens to the `parts` stream and spawns a new task process for each arrival:

```python
@app.arrival_loop("parts")
def parts_generator(context: SimulationContext):
    part_id = 0
    while True:
        # Wait for the next scheduled arrival time
        yield context.wait_for_arrival("parts")

        # Spawn an independent worker task process
        context.spawn(process_part(part_id))
        part_id += 1
```

Next, define the **task process** decorated with `@app.task`. The decorator automatically requests the resource on start, waits a processing time sampled from the `machining` service, and releases it on finish, emitting lifecycle events. The dictionary the function returns is the value of the finished event:

```python
@app.task(service_id="machining", resource_id="lathe")
def process_part(part_id: int):
    # Enforces automatic lock-wait-release lifecycle
    return {"part_id": part_id}
```

### 1.4 Run the simulation

Finally, launch the simulation for 10 seconds of simulated time:

```python
if __name__ == "__main__":
    print("Starting simulation...")
    app.run(until=10.0)
```

When you run this script (`python first_factory.py`), you will see the generated events and telemetry printed directly to the console, demonstrating a complete local simulation.

---

## 2. Adding Randomness and Rules

In this tutorial, you will expand the factory model by adding stochasticity (random service times) and testing dynamic capacity updates.

### 2.1 Introducing Randomness

In real-world factories, machine processing times are never constant. We can register a statistical distribution to represent milling/molding tasks.

Update your `SimulationContext` configuration to:
1. Define a random seed (`random_seed=42`) to guarantee that all random samplings are fully reproducible.
2. Add a `milling` service using a normal distribution (mean of 3.0 seconds, standard deviation of 0.5 seconds).

```python
from dynamic_des import SimulationContext, ConsoleEgress

app = (
    SimulationContext(sim_id="Factory_A", factor=1.0, random_seed=42)
    .add_resource("lathe", current_cap=1, max_cap=5)
    .add_arrival("parts", dist="exponential", rate=0.5)
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    .add_egress(ConsoleEgress())
)
```

### 2.2 Setting Up Dynamic Rules

We can simulate an external control system (like an operator logging a machine online) by scheduling a capacity change using `LocalIngress`. Its delays are wall-clock seconds from the start of the run, which match simulation seconds at `factor=1.0`.

Let's configure the ingress schedule to:
* Start with 1 lathe.
* Increase capacity to 3 at t=10.0 seconds.
* Drop capacity to 2 at t=20.0 seconds.

```python
from dynamic_des import LocalIngress

ingress = LocalIngress(schedule=[
    (10.0, "Factory_A.resources.lathe.current_cap", 3),
    (20.0, "Factory_A.resources.lathe.current_cap", 2)
])

app.add_ingress(ingress)
```

### 2.3 Decorating Tasks with Distributions

Now, update the `@app.task` decorator to point to the `milling` service distribution. The framework will automatically sample from the distribution to dictate the duration of each task:

```python
@app.task(service_id="milling", resource_id="lathe")
def process_part(part_id: int):
    # The timeout delay is now automatically sampled from the 'milling' service config!
    return {"part_id": part_id}
```

### 2.4 Run and Observe

```python
if __name__ == "__main__":
    app.run(until=25.0)
```

When you execute the script, you will notice:
* **Stochastic timings**: Each task takes a slightly different amount of time to complete.
* **Queuing**: In the first 10 seconds, parts pile up because the arrival rate (0.5 parts/sec) exceeds the machine capacity/duration.
* **Capacity Increase**: At t=10s, capacity increases to 3, causing the queue to drain.
* **Seeded determinism**: Because you pinned `random_seed=42`, re-running this script will produce the same random samples and the same `sim_ts` values every time. The `timestamp` field follows the clock the run started at, so it differs between runs unless you pass `logical_start_time`.

---

## 3. Going Distributed (Kafka)

In this final step, you will transition your simulation into a distributed microservice. You will swap local console connectors for Kafka connectors, allowing you to stream telemetry and receive live parameter updates over the network.

### 3.1 Prerequisites and Installation

First, make sure you install the `kafka` optional dependencies:
```bash
pip install "dynamic-des[kafka]"
```

Bring up a local Kafka cluster with [odctl](https://github.com/jaehyeon-kim/odctl):
```bash
odctl up kafka-lite
```

odctl is a separate CLI, installed once with `uv tool install "odctl>=1.0,<2"` or `pip install "odctl>=1.0,<2"`.

### 3.2 Transitioning to Kafka Connectors

Swapping from a local script to a distributed twin is simple: we replace `ConsoleEgress` and `LocalIngress` with `KafkaEgress` and `KafkaIngress` respectively.

Update your configuration code:

```python
import logging
from dynamic_des import SimulationContext, KafkaEgress, KafkaIngress

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s")

# Connect to the local Kafka broker
broker = "localhost:9092"
sim_id = "Factory_A"

app = (
    SimulationContext(sim_id=sim_id, factor=1.0, random_seed=42)
    .add_resource("lathe", current_cap=1, max_cap=5)
    .add_arrival("parts", dist="exponential", rate=0.5)
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    # 1. Add Kafka Ingress to listen to dynamic capacity updates
    .add_ingress(KafkaIngress(
        bootstrap_servers=broker,
        topic=f"{sim_id}-control"
    ))
    # 2. Add Kafka Egress to publish simulation events
    .add_egress(KafkaEgress(
        bootstrap_servers=broker,
        event_topic=f"{sim_id}-events",
        telemetry_topic=f"{sim_id}-telemetry"
    ))
)
```

### 3.3 Keep the Core Logic Identical

Because the standard API decouples infrastructure from business logic, **you do not need to modify any of the simulation generators or task loops** from step 2:

```python
@app.arrival_loop("parts")
def parts_generator(context: SimulationContext):
    part_id = 0
    while True:
        yield context.wait_for_arrival("parts")
        context.spawn(process_part(part_id))
        part_id += 1

@app.task(service_id="milling", resource_id="lathe")
def process_part(part_id: int):
    return {"part_id": part_id}
```

### 3.4 Testing Live Modifications

Start the simulation in your terminal. It will block and run in real time matching the system clock:
```python
if __name__ == "__main__":
    print("Distributed twin running. Press Ctrl+C to stop.")
    app.run()
```

While it is running, you can publish a message to the `Factory_A-control` Kafka topic to dynamically scale the lathe capacity:

```json
{
  "path_id": "Factory_A.resources.lathe.current_cap",
  "value": 3
}
```

The background `KafkaIngress` thread will instantly consume this message, apply the update to the switchboard registry, and the active `DynamicResource` lathe will expand its capacity without stopping the clock or resetting any states.
