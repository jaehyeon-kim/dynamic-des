# Tutorial 1: Your First Factory (Local)

Welcome to the Basics tutorial. In this first step, you will learn the fundamental lifecycle of a digital twin by building a simple factory model that runs locally on your machine.

---

## 1. Setup and imports

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

---

## 2. Initialize the Simulation Context

We will use the **Standard API (`SimulationContext`)** builder to wire up the simulation:
* We define a unique namespace prefix (`sim_id="Factory_A"`).
* We register a resource representing a single machine (`lathe`) with a capacity of 1.
* We configure a static arrival stream named `parts` where a new part arrives every 2 seconds.
* We direct all simulation events to print directly to the console (`ConsoleEgress`).

```python
app = (
    SimulationContext(sim_id="Factory_A", factor=1.0)
    .add_resource("lathe", current_cap=1)
    .add_arrival("parts", dist="exponential", rate=0.5) # 1 part every 2 seconds
    .add_egress(ConsoleEgress())
)
```

---

## 3. Define Simulation Logic

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

Next, define the **task process** decorated with `@app.task`. The decorator automatically requests the resource on start and releases it on finish, emitting telemetry events:

```python
@app.task(service_id=None, resource_id="lathe")
def process_part(part_id: int):
    # Enforces automatic lock-wait-release lifecycle
    yield context.env.timeout(1.5) # Simulates a 1.5-second processing delay
    return {"part_id": part_id}
```

---

## 4. Run the simulation

Finally, launch the simulation for 10 seconds of simulated time:

```python
if __name__ == "__main__":
    print("Starting simulation...")
    app.run(until=10.0)
```

When you run this script (`python first_factory.py`), you will see the generated events and telemetry printed directly to the console, demonstrating a complete local simulation.
