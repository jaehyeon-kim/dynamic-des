import numpy as np

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaEgress,
    KafkaIngress,
    Sampler,
    SimParameter,
)

# 1. Setup Data Model
line_a_params = SimParameter(
    sim_id="Line_A",
    arrival={"standard": DistributionConfig(dist="exponential", rate=0.2)},
    service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},
)

# 2. Setup Kafka Connectors
# Expects JSON: {"path_id": "Line_A.resources.lathe.current_cap", "value": 3}
ingress = KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092")
# Publishes JSON: {"path_id": "Line_A.queue", "value": 2, "timestamp": 10.0}
egress = KafkaEgress(topic="sim-metrics", bootstrap_servers="localhost:9092")

env = DynamicRealtimeEnvironment(factor=1.0)
env.registry.register_sim_parameter(line_a_params)
env.setup_ingress([ingress])
env.setup_egress([egress])

res = DynamicResource(env, "Line_A", "lathe")
sampler = Sampler(rng=np.random.default_rng(42))


# 3. Logic: Service Process
def task_process(env, res, cfg):
    while True:
        yield env.timeout(sampler.sample(line_a_params.arrival["standard"]))

        def work():
            yield res.request()
            yield env.timeout(sampler.sample(cfg))
            res.release()
            # Stream result out to Kafka
            env.publish("Line_A.lathe.finished", 1)

        env.process(work())


# 4. Logic: Telemetry (Egress)
def monitor(env, res):
    while True:
        env.publish("Line_A.lathe.queue_length", len(res.queue.items))
        yield env.timeout(5.0)


# 5. Run
env.process(task_process(env, res, line_a_params.service["milling"]))
env.process(monitor(env, res))

print("Simulation started. Listening on Kafka topic 'sim-config'...")
env.run(until=60)
