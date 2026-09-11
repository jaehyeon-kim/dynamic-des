# Local Simulation (Low-Level Imperative API)

This example demonstrates how to build a dynamic simulation using the low-level **Imperative API** and **Local Connectors**.

Local connectors do not require Docker, Kafka, or any external data stores. They are perfect for testing, benchmarking, or scenarios where parameter changes need to occur at specific wall-clock intervals deterministically.

---

## Quick Start

```bash
# Run the imperative simulation (no infrastructure required)
uv run ddes-imperative-local
```

## Full Source Code

This script initializes a production line, schedules a capacity update to happen 10 seconds into the future, and streams telemetry directly to your terminal using raw SimPy generators.

```python
--8<-- "src/dynamic_des/examples/imperative/local_example.py"
```
