# Local Simulation (Standard Declarative API)

This example demonstrates how to build a dynamic simulation using the declarative **Standard API (`SimulationContext`)** and **Local Connectors**.

Local connectors do not require Docker, Kafka, or any external data stores. They are perfect for testing, benchmarking, or scenarios where parameter changes need to occur at specific wall-clock intervals deterministically.

---

## Quick Start

```bash
# Run the declarative simulation (no infrastructure required)
uv run ddes-local
```

## Full Source Code

This script initializes a production line, runs it for 60 simulation seconds, and streams events and telemetry directly to your terminal.

```python
--8<-- "src/dynamic_des/examples/declarative/local_example.py"
```
