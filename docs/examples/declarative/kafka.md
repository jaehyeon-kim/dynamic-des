# Kafka Digital Twin (Standard Declarative API)

This example demonstrates how to integrate `dynamic-des` into a full event-driven architecture using the declarative **Standard API (`SimulationContext`)**.

By replacing the local connectors with `KafkaIngress` and `KafkaEgress`, the simulation becomes a fully detached microservice. It listens for external JSON commands to mutate its state, and streams telemetry and strictly-typed Pydantic events to outbound topics.

---

## Quick Start

```bash
# 1. Spin up the Kafka broker and schema registry via Docker Compose
uv run ddes-kafka-infra-up

# 2. Run the declarative simulation (Ctrl + C to stop)
uv run ddes-kafka

# 3. In a second terminal, watch and steer the run from the dashboard
#    (opens at http://localhost:8080, Ctrl + C to stop)
uv run ddes-kafka-dashboard

# 4. Clean up the infrastructure when finished
uv run ddes-kafka-infra-down
```

## Full Source Code

This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

```python
--8<-- "src/dynamic_des/examples/declarative/kafka_example.py"
```
