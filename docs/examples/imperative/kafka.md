# Kafka Digital Twin (Low-Level Imperative API)

This example demonstrates how to integrate `dynamic-des` into a full event-driven architecture using the low-level **Imperative API**.

By replacing the Local connectors with `KafkaIngress` and `KafkaEgress`, the simulation becomes a fully detached microservice. It listens for external JSON commands to mutate its state, and streams telemetry and strictly-typed Pydantic events to outbound topics.

---

## Quick Start

```bash
# 1. Spin up the Kafka broker and schema registry with odctl
odctl up kafka-lite

# 2. Run the imperative simulation (Ctrl + C to stop)
uv run ddes-imperative-kafka

# 3. Clean up the infrastructure when finished
odctl down kafka-lite --volumes
```

## Full Source Code

This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

```python
--8<-- "src/dynamic_des/examples/imperative/kafka_example.py"
```
