# In-Memory Store (Redis) Imperative API

This example demonstrates how to integrate `dynamic-des` with a high-performance Redis cache using the low-level **Imperative API (`DDESEnv`)**.

This is useful if you are migrating existing SimPy generators and prefer to handle `env.process()` and component registration manually rather than using the Builder Pattern.

---

## 1. Quick Start

You can easily run this demo using the pre-configured CLI entry points. 

```bash
# 1. Spin up the Redis/Valkey database via Docker Compose
uv run ddes-redis-infra-up

# 2. Run the imperative simulation
uv run ddes-imperative-redis
```

**In a second terminal, execute the dynamic parameter update:**
```bash
# Connect to the Redis container and publish the parameter update
docker exec -it redis valkey-cli PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
```

```bash
# 3. Clean up the infrastructure when finished
uv run ddes-redis-infra-down
```

---

## Full Source Code

```python
--8<-- "src/dynamic_des/examples/imperative/redis_example.py"
```
