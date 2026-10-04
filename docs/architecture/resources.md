# Resources and Containers

Standard SimPy objects are static. Dynamic DES wraps SimPy objects so their capacity follows the registry in real time.

---

## 1. Discrete Resources (`DynamicResource`)

`DynamicResource` is built from a SimPy `PriorityStore` request queue and a `Container` token pool, and represents discrete assets that process individual tasks (e.g. machines, bays, or operators).

### Shrinking Safety Guarantee
When capacity increases, tokens are immediately added to the pool. When capacity shrinks (e.g., from 5 to 2 due to an unexpected machine breakdown):
* If 4 machines are currently processing tasks, **Dynamic DES will not terminate active processes**.
* The resource enters a temporary "over-capacity" state where it waits for tasks to finish.
* As tasks complete and release their tokens, `DynamicResource` intercepts and discards them until the capacity level naturally drops to the targeted limit (2).

---

## 2. Continuous Containers (`DynamicContainer`)

`DynamicContainer` wraps a SimPy `Container` and represents continuous quantities (e.g. fuel tank levels, conveyor queues, or concept drift/physical wear).

* **Capacity**: The physical size limit of the tank or container. It follows the registry.
* **Level**: The current fluid level or quantity of material. It changes only through `put` and `get`.

---

## 3. Dynamic Stores (`DynamicStore`)

`DynamicStore` wraps a SimPy `Store`, or a `PriorityStore` when priorities are needed, and represents item-based collections (e.g. warehouses, buffer areas, or order books).

---

## Creating Them

`SimulationContext.run()` creates a `DynamicResource` for every `add_resource`. It does not create containers or stores: `add_container` registers the capacity paths only. A process that needs a container builds it from the registered paths. `DynamicContainer` and `DynamicStore` are imported from `dynamic_des`, like `DynamicResource`. Stores have no builder method; register them with `SimParameter(stores=...)` on the low-level API.

```python
from dynamic_des import DynamicContainer, SimulationContext

# Declare resources and containers in SimulationContext
app = (
    SimulationContext(sim_id="Line_A")
    .add_resource("lathe", current_cap=2, max_cap=5)
    .add_container("fuel_tank", current_cap=100.0, max_cap=500.0)
)

def refuel(context):
    # The container reads its capacity from Line_A.containers.fuel_tank
    tank = DynamicContainer(context.env, "Line_A", "fuel_tank", init=50.0)
    while True:
        yield context.env.timeout(10.0)
        yield tank.put(25.0)
        context.publish("fuel_tank.level", tank.level)

app.add_process(refuel)
```
