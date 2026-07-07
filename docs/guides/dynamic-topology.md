# Dynamic Topology (Absolute Edge Cases)

The Standard API (`SimulationContext`) relies on an **immutability constraint**: all physical resources, service distribution schemas, and I/O connections must be fully configured before the clock starts running.

In some rare digital twin applications, the physical layout itself changes dynamically during execution (e.g., a shipping dock provisions a new bay, or a network threshold triggers the dynamic creation of new Kafka topics and data formats mid-simulation).

If your twin requires **dynamic topology mutation**, you must bypass `SimulationContext` and write purely imperative code against the low-level `DynamicRealtimeEnvironment` and the flat registry keys.

---

## Code Example: Mutating the Infrastructure Registry Mid-Run

This example shows how to dynamically register a brand new resource and update flat paths in the registry mid-simulation.

```python
import logging
import numpy as np
from dynamic_des import (
    DynamicRealtimeEnvironment, 
    DynamicResource, 
    Sampler, 
    ConsoleEgress,
    CapacityConfig,
    SimParameter
)

logging.basicConfig(level=logging.INFO)

# 1. Start with a minimal initial configuration
initial_params = SimParameter(
    sim_id="Line_A",
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)}
)

env = DynamicRealtimeEnvironment(factor=1.0)
env.registry.register_sim_parameter(initial_params)
env.setup_egress([ConsoleEgress()])

# Active resources map
resources = {
    "lathe": DynamicResource(env, "Line_A", "lathe")
}

def worker(env, resource_name: str, task_id: int):
    res = resources[resource_name]
    with res.request() as req:
        yield req
        print(f"Task {task_id} started on {resource_name} (capacity: {res.capacity})")
        yield env.timeout(3.0)
        print(f"Task {task_id} finished on {resource_name}")

def supervisor(env):
    """
    Acts as the topology supervisor, adding new physical machines
    when queues become too long.
    """
    task_id = 0
    while True:
        yield env.timeout(4.0)
        
        # At t=12.0s, dynamically provision a second resource 'molder' 
        # that didn't exist when the simulation started!
        if env.now >= 12.0 and "molder" not in resources:
            print("--- Supervisor: Registering new resource 'molder' ---")
            
            # 1. Update the schema config in the registry
            env.registry.update_value("Line_A.resources.molder.max_cap", 5)
            env.registry.update_value("Line_A.resources.molder.current_cap", 2)
            
            # 2. Instantiate and register the new SimPy DynamicResource object
            resources["molder"] = DynamicResource(env, "Line_A", "molder")
            
        # Spawn tasks on the lathe
        env.process(worker(env, "lathe", task_id))
        
        # If the molder is available, also route tasks to it
        if "molder" in resources:
            env.process(worker(env, "molder", task_id))
            
        task_id += 1

if __name__ == "__main__":
    env.process(supervisor(env))
    env.run(until=20.0)
```
