# Simulation Context

The `SimulationContext` acts as the entry point and configuration builder for standard Dynamic DES simulations. It implements the **Builder Pattern** to construct the simulation environment and register resources, statistical samplers, and ingress/egress connectors.

---

## Builder Lifecycle

Creating a simulation with `SimulationContext` follows a strict two-phase lifecycle to guarantee **thread safety** and **determinism**:

```text
1. Builder Phase (Configure)
   └── Register resources, distributions, and ingress/egress
2. Compilation Phase (app.run)
   ├── Instantiate DynamicRealtimeEnvironment
   ├── Start background network/egress threads
   └── Boot the SimPy clock and event loop
```

1. **Builder Phase (Pre-Compilation)**: You register resources, services, and connectors. Everything is held as passive configuration data structures (`SimParameter`, `DistributionConfig`, etc.) in memory.
2. **Compilation & Execution Phase (Post-Compilation)**: The moment you call `app.run(...)`, the builder compiles the configuration. It instantiates the `DynamicRealtimeEnvironment`, boots background network connector threads, and starts the SimPy event loop.

> [!IMPORTANT]
> **Architectural Guarantee**: You cannot call runtime helpers like `context.spawn()`, `context.get_resource()`, or `context.env` during the Builder Phase. Attempting to do so will raise a `RuntimeError`. This prevents partial state leaks and guarantees that the environment clock is strictly controlled.

---

## Builder API Reference

The fluent builder API allows chaining configurations:

### Resource Configuration
* `.add_resource(name, current_cap, max_cap)`: Stages a discrete token-based resource (e.g. machines, work areas).
* `.add_container(name, current_cap, max_cap)`: Stages a continuous fluid-like container (e.g. storage tanks, battery charge levels).
* `.add_variable(name, value)`: Stages generic variables or physical parameters (e.g. system conveyor speeds).

### Statistical Profiles
* `.add_arrival(name, dist, rate, mean)`: Configures an inter-arrival time distribution.
* `.add_service(name, dist, rate, mean, std)`: Configures a task processing duration distribution.

### Connectors & Ingestion
* `.add_ingress(provider)`: Attaches an ingress connector (e.g. `LocalIngress` or `KafkaIngress`) to stream live configuration updates into the switchboard.
* `.add_egress(provider)`: Attaches an egress connector (e.g. `ConsoleEgress` or `KafkaEgress`) to publish event and telemetry streams.
* `.with_batching(batch_size, flush_interval)`: Tunes the internal queue batching size and flush timeouts for highly efficient I/O.
