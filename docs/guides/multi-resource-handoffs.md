# Multi-Resource Handoffs (Hybrid Pattern)

While the `@app.task` decorator abstracts resource locking, it enforces a strict **single-resource lifecycle** (`request -> wait -> release`). 

If your simulation requires a task to lock multiple resources concurrently or orchestrate a handover (e.g., requesting a crane, moving a part, locking a machine, and then releasing the crane while keeping the machine locked), you must use the **Hybrid Pattern** by spawning a custom generator.

---

## Overlapping Lock Limit

In many production facilities, physical objects cannot transition between stations without a transfer mechanism (like a crane or robotic arm):

```text
1. Part requests Crane ──────> Crane is acquired and locked.
2. Part requests Machine ────> Blocks until Machine is free (Crane remains locked).
3. Machine is acquired ─────-> Part releases Crane (Handoff complete).
4. Part processes on Machine.
5. Part releases Machine.
```

---

## Code Example: Spawning a Custom Handover Generator

Instead of using `@app.task`, you can write a raw generator function, retrieve resource handles via `context.get_resource()`, and schedule the processes dynamically with `context.spawn()`.

```python
import logging
from dynamic_des import SimulationContext, ConsoleEgress

logging.basicConfig(level=logging.INFO)

app = (
    SimulationContext(sim_id="Line_A", factor=1.0)
    .add_resource("crane", current_cap=1)
    .add_resource("mill", current_cap=1)
    .add_arrival("standard", dist="exponential", rate=1.0)
    .add_egress(ConsoleEgress())
)

# 1. Custom process logic handling concurrent locks
def handoff_worker(context: SimulationContext, part_id: int):
    crane = context.get_resource("crane")
    mill = context.get_resource("mill")
    
    task_key = f"task-{part_id}"
    context.env.publish_event(task_key, {"status": "queued"})
    
    # Lock the Crane for transport
    with crane.request() as crane_req:
        yield crane_req
        context.env.publish_event(task_key, {"status": "crane_loaded"})
        yield context.env.timeout(1.0)  # Transport delay
        
        # Request the Mill while still holding the Crane!
        with mill.request() as mill_req:
            yield mill_req
            context.env.publish_event(task_key, {"status": "mill_entered"})
            
            # The Crane is automatically released here as we exit the outer block,
            # but the Mill remains locked!
            
        # Perform milling operation
        yield context.env.timeout(4.0)
        context.env.publish_event(task_key, {"status": "finished"})

# 2. Wire the custom generator to the arrival loop
@app.arrival_loop("standard")
def arrival_loop(context: SimulationContext):
    part_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        # Spawn the custom handover process
        context.spawn(handoff_worker(context, part_id))
        part_id += 1

if __name__ == "__main__":
    app.run(until=30.0)
```
