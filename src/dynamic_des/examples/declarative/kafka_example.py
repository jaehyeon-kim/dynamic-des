import logging
import os
import time

from pydantic import BaseModel

from dynamic_des import (
    KafkaAdminConnector,
    KafkaEgress,
    KafkaIngress,
    SimulationContext,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
)
logger = logging.getLogger("kafka_example")


# ==========================================
# 1. Define Strongly-Typed Event Payloads
# ==========================================
class TaskEvent(BaseModel):
    """
    Strongly typed event payload to guarantee schema consistency
    when shipping data over the wire to Kafka.
    """

    path_id: str
    status: str


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ==========================================
# 2. Declarative Infrastructure Builder
# ==========================================
app = (
    SimulationContext(sim_id="Line_A", factor=1.0, random_seed=42)
    .add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS))
    .add_egress(
        KafkaEgress(
            event_topic="sim-events",
            telemetry_topic="sim-telemetry",
            bootstrap_servers=BOOTSTRAP_SERVERS,
        )
    )
    .add_resource("lathe", current_cap=1, max_cap=10)
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    .add_arrival("standard", dist="exponential", rate=1.0)
)


# ==========================================
# 3. Simulation Logic
# ==========================================
@app.task(service_id="milling", resource_id="lathe")
def process_part(task_id: int, context):
    """
    The @task decorator automatically locks the resource and emits the
    'queued' and 'started' events. We just execute our custom logic
    and return the final payload.
    """
    logger.info(f"Task {task_id} started at sim time: {context._env.now:.2f}s")

    # Return the strongly-typed Pydantic model for the 'finished' state
    return TaskEvent(path_id="Line_A.service.milling", status="finished").model_dump(
        mode="json"
    )


@app.arrival_loop("standard")
def arrival_generator(context):
    task_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(process_part(task_id, context))
        task_id += 1


@app.telemetry_loop(interval=2.0)
def telemetry_monitor(context):
    """Low-volume system health stream."""
    res = context.get_resource("lathe")

    # Exact parity with the imperative telemetry outputs
    context.publish("lathe.capacity", res.capacity)
    context.publish("lathe.in_use", res.in_use)
    context.publish("lathe.queue_length", len(res.queue.items))

    util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
    context.publish("lathe.utilization", util)

    avg_wait = len(res.queue.items) * 3.0
    context.publish("lathe.avg_wait", avg_wait)


# ==========================================
# 4. Execution
# ==========================================
def run():
    TOPICS_CONFIG = [
        {"name": "sim-config", "partitions": 1},
        {"name": "sim-telemetry", "partitions": 1},
        {"name": "sim-events", "partitions": 1},
    ]

    logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    try:
        admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
        admin.create_topics(topics_config=TOPICS_CONFIG)
        time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not explicitly create topics: {e}")

    print("Simulation started.")
    print("  - Listen to 'sim-telemetry' for system vitals.")
    print("  - Listen to 'sim-events' for task lifecycles.")
    print("  - Send to 'sim-config' to update parameters.")

    app.run()  # Starts the clock and orchestrates all connectors


if __name__ == "__main__":
    run()
