# Preemptive Machine Breakdowns (Hybrid Pattern)

In manufacturing simulations, machine breakdowns are usually **preemptive**: they occur randomly and interrupt whatever task is currently running.

The `@app.task` decorator abstracts the internal `yield env.timeout()` execution block. Therefore, you cannot wrap it in a standard Python `try/except` block to catch a SimPy `simpy.Interrupt`. If you need to simulate breakdowns and model how machines recover, you must write a custom generator and use a background failure loop (the "Chaos Monkey" pattern).

---

## Code Example: Simulating Preemptive Interruption

This script defines a custom worker process that can catch a SimPy `Interrupt` mid-operation, report the failure to Kafka/Console, and wait for a repair crew before retrying.

```python
import logging
import simpy
from dynamic_des import SimulationContext, ConsoleEgress

logging.basicConfig(level=logging.INFO)

app = (
    SimulationContext(sim_id="Line_A", factor=1.0)
    .add_resource("lathe", current_cap=1)
    .add_arrival("standard", dist="exponential", rate=0.2)
    .add_egress(ConsoleEgress())
)

# Keep track of active task processes to interrupt
active_processes = []

def worker_task(context: SimulationContext, task_id: int):
    lathe = context.get_resource("lathe")
    task_key = f"task-{task_id}"
    context.env.publish_event(task_key, {"status": "queued"})
    
    # Track this running SimPy process
    active_processes.append(context.env.active_process)
    
    try:
        with lathe.request() as req:
            yield req
            context.env.publish_event(task_key, {"status": "started"})
            
            # Simulated processing time
            yield context.env.timeout(5.0)
            
            context.env.publish_event(task_key, {"status": "finished"})
    except simpy.Interrupt as interrupt:
        # Catch machine failure!
        cause = interrupt.cause
        context.env.publish_event(
            task_key, 
            {"status": "interrupted", "error": f"Machine breakdown: {cause}"}
        )
        
        # Simulates waiting for maintenance/repair
        yield context.env.timeout(10.0)
        context.env.publish_event(task_key, {"status": "machine_repaired"})
    finally:
        if context.env.active_process in active_processes:
            active_processes.remove(context.env.active_process)

# 3. Chaos Daemon (Simulates random machine breakdowns)
def chaos_monkey(context: SimulationContext):
    while True:
        # Trigger a breakdown every 12 seconds
        yield context.env.timeout(12.0)
        
        if active_processes:
            # Interrupt the currently active task
            target = active_processes[0]
            target.interrupt("Overheating alarm!")

# 4. Wire everything
@app.arrival_loop("standard")
def arrival_loop(context: SimulationContext):
    task_id = 0
    # Start the chaos monkey loop
    context.spawn(chaos_monkey(context))
    
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(worker_task(context, task_id))
        task_id += 1

if __name__ == "__main__":
    app.run(until=40.0)
```
