# In-Memory Store (Redis) Declarative API

This example demonstrates how to integrate `dynamic-des` with a high-performance Redis cache using the declarative **Standard API (`SimulationContext`)**.

By combining `RedisIngress` and `RedisEgress`, your simulation can achieve sub-millisecond latency for both reading dynamic parameters via Pub/Sub and writing high-throughput telemetry data via Redis Streams.

---

## 1. Streaming to Redis

When generating events, `RedisEgress` allows you to stream outputs directly into Redis Streams (`XADD`). Just like other connectors, it supports **Stream Multiplexing** by reading the `__stream__` key from your output dictionaries.

```python
app = (
    SimulationContext(sim_id="Factory", factor=1.0)
    .add_egress(RedisEgress(REDIS_URL, stream_name="default_events"))
)

# ... inside the generator
part_event = {
    "__stream__": "part_events",
    "part_id": 1,
    "status": "arrived",
}
context.publish("factory_event", part_event)
```

## 2. Dynamic Parameter Updates (Ingress)

This example attaches a `RedisIngress` listening to a Pub/Sub channel called `simulation_params`. While the simulation is running, you can dynamically update parameters (like speeding up the arrival rate) by simply publishing a JSON string to the channel!

```python
app.add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
```

## 3. Quick Start

You can easily run this demo using the pre-configured CLI entry points. **To test the dynamic ingress updates**, open a second terminal while the simulation is running and execute the `PUBLISH` command below.

```bash
# 1. Spin up the Redis/Valkey database via Docker Compose
uv run ddes-redis-infra-up

# 2. Run the declarative simulation
uv run ddes-redis
```

**In a second terminal, execute the dynamic parameter update:**
```bash
# Connect to the Redis container and publish the parameter update
docker exec -it redis valkey-cli PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
```
*You will immediately see the simulation terminal log that the update was ingested and start generating parts much faster!*

```bash
# 3. Clean up the infrastructure when finished
uv run ddes-redis-infra-down
```

---

## Full Source Code

```python
--8<-- "src/dynamic_des/examples/declarative/redis_example.py"
```
