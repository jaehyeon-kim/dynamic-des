# Resources and Containers

Standard SimPy objects are static. Dynamic DES introduces dynamic wrapper classes that subscribe directly to the Registry to update their capacity, consumption, or contents in real time.

---

## 1. Discrete Resources (`DynamicResource`)

`DynamicResource` wraps a SimPy `Resource` and represents discrete assets that process individual tasks (e.g. machines, bays, or operators).

### Shrinking Safety Guarantee
When capacity increases, tokens are immediately added to the pool. When capacity shrinks (e.g., from 5 to 2 due to an unexpected machine breakdown):
* If 4 machines are currently processing tasks, **Dynamic DES will not terminate active processes**.
* The resource enters a temporary "over-capacity" state where it waits for tasks to finish.
* As tasks complete and release their tokens, `DynamicResource` intercepts and discards them until the capacity level naturally drops to the targeted limit (2).

---

## 2. Continuous Containers (`DynamicContainer`)

`DynamicContainer` wraps a SimPy `Container` and represents continuous quantities (e.g. fuel tank levels, conveyor queues, or concept drift/physical wear).

Continuous states are updated dynamically via:
* **Level**: The current fluid level or quantity of material.
* **Capacity**: The physical size limit of the tank/container.

---

## 3. Dynamic Stores (`DynamicStore`)

`DynamicStore` wraps a SimPy `FilterStore` and represents item-based collections with filtering capabilities (e.g. warehouses, buffer areas, or order books).

```python
# Declare resources and containers in SimulationContext
app = (
    SimulationContext(sim_id="Line_A")
    .add_resource("lathe", current_cap=2, max_cap=5)
    .add_container("fuel_tank", current_cap=100.0, max_cap=500.0)
)
```
