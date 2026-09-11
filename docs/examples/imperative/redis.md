# In-Memory Store (Redis) Imperative API

This example demonstrates how to integrate `dynamic-des` with a high-performance Redis cache using the low-level **Imperative API (`DDESEnv`)**.

This is useful if you are migrating existing SimPy generators and prefer to handle `env.process()` and component registration manually rather than using the Builder Pattern.

---

## 1. Quick Start

You can easily run this demo using the pre-configured CLI entry points.

```bash
# 1. Spin up the Valkey database with odctl
odctl up valkey

# 2. Grant the `user` account access to Pub/Sub channels.
#    odctl creates it with `~* +@all`, which covers keys and commands but not
#    channels, so RedisIngress is refused with NOPERM until this runs.
docker exec -it valkey valkey-cli --user user --pass password ACL SETUSER user allchannels

# 3. Run the imperative simulation
uv run ddes-imperative-redis
```

**In a second terminal, execute the dynamic parameter update:**
```bash
# Connect to the Valkey container and publish the parameter update
docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
```

```bash
# 4. Clean up the infrastructure when finished
odctl down valkey --volumes
```

---

## Full Source Code

```python
--8<-- "src/dynamic_des/examples/imperative/redis_example.py"
```
