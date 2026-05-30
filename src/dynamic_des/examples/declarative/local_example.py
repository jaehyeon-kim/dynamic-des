import logging

from dynamic_des import ConsoleEgress, SimulationContext

logger = logging.getLogger(__name__)

# ==========================================
# 1. Declarative Infrastructure Builder
# ==========================================
app = (
    SimulationContext(sim_id="Factory_A", factor=1.0)
    .add_egress(ConsoleEgress())
    .add_resource("lathe", current_cap=2, max_cap=5)
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    .add_arrival("standard", dist="exponential", rate=1.0)
)


# ==========================================
# 2. Simulation Logic (Decorators)
# ==========================================
@app.task(service_id="milling", resource_id="lathe")
def process_part(task_id: int):
    """Executes the milling service and returns the custom payload."""
    return {"event_type": "part_produced", "part_id": task_id, "quality": "A"}


@app.arrival_loop("standard")
def arrival_generator(context):
    """Continuously spawns new parts based on the 'standard' arrival distribution."""
    task_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(process_part(task_id))
        task_id += 1


@app.telemetry_loop(interval=2.0)
def telemetry_generator(context):
    """Samples the hidden state of the resources every 2 simulation seconds."""
    res = context.get_resource("lathe")
    util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0

    context.publish("utilization", util)
    context.publish("queue_length", len(res.queue.items))


# ==========================================
# 3. Execution
# ==========================================
def run():
    """Starts the local simulation for a fixed duration."""
    logger.info("Starting Declarative Local Example. Running for 60 seconds...")
    app.run(until=60)


if __name__ == "__main__":
    run()
