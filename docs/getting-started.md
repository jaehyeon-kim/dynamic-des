# Getting Started

Ready to build real-time digital twins? This guide will walk you through installing Dynamic DES, running the built-in zero-setup demos, and exploring the core infrastructure.

## Installation

Install the core library:

```bash
pip install dynamic-des
```

To include specific backends:

```bash
# For Kafka support
pip install "dynamic-des[kafka]"

# For Confluent Schema Registry (Avro)
pip install "dynamic-des[kafka,confluent]"

# For AWS Glue Schema Registry (Avro)
pip install "dynamic-des[kafka,glue]"

# For all backends (Kafka, Redis, Postgres, Dashboard, Avro)
pip install "dynamic-des[all]"
```

---

## Quick Start: Zero-Setup Demos

Dynamic DES comes with built-in examples and infrastructure orchestration so you can see it in action immediately. You do not need to write a single line of code to test this out.

**1. Run the local, dependency-free simulation:**

```bash
ddes-local-example
```

**2. Run the full Real-Time Digital Twin stack with Kafka and a live UI:**

```bash
# Start the background Kafka cluster (requires Docker)
ddes-kafka-infra-up

# Open a new terminal and run the simulation
# Ctrl + C to stop
ddes-kafka-example

# Open a new terminal and start the monitoring dashboard (opens in browser)
# Visit http://localhost:8080
# Ctrl + C to stop
ddes-kafka-dashboard

# Clean up the infrastructure when finished
ddes-kafka-infra-down
```

## Build Your Own

Ready to build your own system? We have prepared a gallery of real-world use cases to demonstrate how to architect your simulation.

- [Local Simulation](examples/local.md): A dependency-free approach to testing.
- [Kafka Digital Twin](examples/kafka.md): A full manufacturing architecture with dynamic queues.
<!-- - [Crypto Trading Bot](): Simulating order books, slippage, and API rate limits.
- [RPG Adventure Game](): Managing live game states, monster spawns, and server capacity. -->
