# Tutorial 2: Adding Randomness and Rules

In this tutorial, you will expand the factory model by adding stochasticity (random service times) and testing dynamic capacity updates.

---

## 1. Introducing Randomness

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

---

## 2. Setting Up Dynamic Rules

We can simulate an external control system (like an operator logging a machine online) by scheduling a capacity change using `LocalIngress`.

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

---

## 3. Decorating Tasks with Distributions

Now, update the `@app.task` decorator to point to the `milling` service distribution. The framework will automatically sample from the distribution to dictate the duration of each task:

```python
@app.task(service_id="milling", resource_id="lathe")
def process_part(part_id: int):
    # The timeout delay is now automatically sampled from the 'milling' service config!
    return {"part_id": part_id}
```

---

## 4. Run and Observe

```python
if __name__ == "__main__":
    app.run(until=25.0)
```

When you execute the script, you will notice:
* **Stochastic timings**: Each task takes a slightly different amount of time to complete.
* **Queuing**: In the first 10 seconds, parts pile up because the arrival rate (0.5 parts/sec) exceeds the machine capacity/duration.
* **Capacity Increase**: At t=10s, capacity increases to 3, causing the queue to clear instantly.
* **Seeded determinism**: Because you pinned `random_seed=42`, re-running this script will produce the exact same timestamps and random samples every time.
