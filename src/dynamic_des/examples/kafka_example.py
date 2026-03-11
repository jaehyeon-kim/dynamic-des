import time

import numpy as np

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaAdminConnector,
    KafkaEgress,
    KafkaIngress,
    Sampler,
    SimParameter,
)

# 0. Create Kafka topics
BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS_CONFIG = [
    {"name": "sim-config", "partitions": 1},
    {"name": "sim-telemetry", "partitions": 1},
    {"name": "sim-events", "partitions": 1},
]

admin_connector = KafkaAdminConnector(
    bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100
)
admin_connector.create_topics(topics_config=TOPICS_CONFIG)

time.sleep(2)

# 1. Define initial system state
line_a_params = SimParameter(
    sim_id="Line_A",
    arrival={
        "standard": DistributionConfig(dist="exponential", rate=1.0)
    },  # 1 every 1s
    service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},
)

# 2. Setup Environment with Kafka Connectors
# Expects JSON: {"path_id": "Line_A.resources.lathe.current_cap", "value": 3}
ingress = KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092")
# Publishes JSON: {"path_id": "Line_A.queue", "value": 2, "timestamp": 10.0}
egress = KafkaEgress(
    telemetry_topic="sim-telemetry",
    event_topic="sim-events",
    bootstrap_servers="localhost:9092",
)

env = DynamicRealtimeEnvironment(factor=1.0)
env.registry.register_sim_parameter(line_a_params)
env.setup_ingress([ingress])
env.setup_egress([egress])

# 3. Initialize Resources and Sampler
res = DynamicResource(env, "Line_A", "lathe")
sampler = Sampler(rng=np.random.default_rng(42))


# 4. Define Simulation Logic
def arrival_process(env: DynamicRealtimeEnvironment, res: DynamicResource):
    arrival_cfg = env.registry.get_config("Line_A.arrival.standard")
    service_path = "Line_A.service.milling"
    task_id = 0

    while True:
        yield env.timeout(sampler.sample(arrival_cfg))
        env.process(work_task(env, task_id, res, service_path))
        task_id += 1


def work_task(
    env: DynamicRealtimeEnvironment, task_id: int, res: DynamicResource, path_id: str
):
    task_key = f"task-{task_id}"

    # High-volume event stream
    env.publish_event(task_key, {"path_id": path_id, "status": "queued"})

    with res.request() as req:
        yield req

        env.publish_event(task_key, {"path_id": path_id, "status": "started"})

        # Use latest service config from registry
        service_cfg = env.registry.get_config(path_id)
        yield env.timeout(sampler.sample(service_cfg))

        env.publish_event(task_key, {"path_id": path_id, "status": "finished"})


def telemetry_monitor(env: DynamicRealtimeEnvironment, res: DynamicResource):
    """Low-volume system health stream."""
    while True:
        # Pushed to 'sim-telemetry' topic
        env.publish_telemetry("Line_A.lathe.capacity", res._capacity)
        env.publish_telemetry("Line_A.lathe.in_use", res.in_use)
        env.publish_telemetry("Line_A.lathe.queue_length", len(res.queue.items))

        util = (res.in_use / res._capacity) * 100 if res._capacity > 0 else 0
        env.publish_telemetry("Line_A.lathe.utilization", util)

        yield env.timeout(2.0)


# 5. Run
env.process(arrival_process(env, res))
env.process(telemetry_monitor(env, res))

print("Simulation started.")
print("  - Listen to 'sim-telemetry' for system vitals.")
print("  - Listen to 'sim-events' for task lifecycles.")
print("  - Send to 'sim-config' to update parameters.")

env.run()
