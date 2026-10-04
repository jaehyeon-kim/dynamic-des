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

> **Architectural Guarantee**: You cannot call runtime helpers like `context.spawn()`, `context.get_resource()`, or `context.env` during the Builder Phase. Attempting to do so will raise a `RuntimeError`. This prevents partial state leaks and guarantees that the environment clock is strictly controlled.

---

## Builder API Reference

The fluent builder API allows chaining configurations:

### Resource Configuration
* `.add_resource(name, current_cap, max_cap)`: Stages a discrete token-based resource (e.g. machines, work areas).
* `.add_container(name, current_cap, max_cap)`: Stages a continuous fluid-like container (e.g. storage tanks, battery charge levels).
* `.add_variable(name, value)`: Stages generic variables or physical parameters (e.g. system conveyor speeds).

### Clock and Pacing
* `SimulationContext(sim_id, factor=1.0, random_seed=None, logical_start_time=None)`: `factor` sets the pacing (0.0 runs unpaced), `random_seed` seeds the shared `Sampler`, and `logical_start_time` sets the instant that simulation time 0 maps to in every record's `timestamp`.
* `SimulationContext(..., go_live_at)`: Logical instant, a datetime on the same clock as `logical_start_time`, at which `factor` gives way to real-time pacing. One run can therefore backfill unpaced and then tail live. See [Backfill Then Go Live in One Run](../guides/backfill-then-live.md).

### Statistical Profiles
* `.add_arrival(name, dist, rate, mean, std)`: Configures an inter-arrival time distribution.
* `.add_service(name, dist, mean, std, rate)`: Configures a task processing duration distribution.

### Decorators
* `@app.task(service_id, resource_id)`: Wraps a function into a task that emits `queued`, waits for the resource, emits `started`, waits a time sampled from the service, and emits the finished event. The finished event's value is exactly the dictionary the function returns, so return `status` or `path_id` yourself if consumers need them.
* `@app.arrival_loop(arrival_id)`: Starts a generator function with the context when the run starts, typically a loop over `context.wait_for_arrival(arrival_id)`.
* `@app.telemetry_loop(interval)`: Calls a function with the context every `interval` simulation seconds.

### Processes
* `.add_process(func, **kwargs)`: Starts a generator function when the run starts, called as `func(context, **kwargs)`. Use it for a process that is not an arrival or telemetry loop, such as a drift engine. It is the builder form of `context.spawn()`, which only works once the run has started.
* `.compile_parameters()`: Returns the `SimParameter` that `run()` registers, so the registry paths a configuration creates can be checked before the run.

### Connectors & Ingestion
* `.add_ingress(provider)`: Attaches an ingress connector (e.g. `LocalIngress` or `KafkaIngress`) to stream live configuration updates into the switchboard.
* `.add_egress(provider, when=None, batch_size=None, flush_interval=None)`: Attaches an egress connector (e.g. `ConsoleEgress` or `KafkaEgress`) to publish event and telemetry streams. Every attached provider receives every record, so a stream sink and a lake sink can be written in one pass. Pass `when` to give a provider a predicate and route records instead, for example the hot tail to Kafka and cold history to Parquet. Pass `batch_size` or `flush_interval` to give one provider its own cadence, so a stream sink can flush small and often while a lake sink writes large files.
* `.with_batching(batch_size, flush_interval)`: Sets the default queue batching size and flush timeout for highly efficient I/O. Every provider uses these unless `add_egress` overrides them.
* `.with_batching(..., max_queued_batches, drain_stall_seconds)`: Bounds the egress queue and sets how long teardown keeps waiting for it. The queue is bounded so a sink that cannot keep up slows the simulation instead of building a backlog, and teardown drains until the queue stops shrinking rather than abandoning it on a fixed deadline. A sink that stops consuming altogether raises `RuntimeError` rather than losing events silently.

---

## Registry Paths

`run()` flattens the configuration into the registry, and every path an ingress message, a scenario or `registry.get` names has one of these forms:

| Builder call | Paths |
|---|---|
| `add_arrival(name, ...)`, `add_service(name, ...)` | `<sim_id>.arrival.<name>.rate` and `<sim_id>.service.<name>.rate` for an exponential distribution, otherwise `.mean` and `.std` |
| `add_resource(name, ...)`, `add_container(name, ...)` | `<sim_id>.resources.<name>.current_cap` and `.max_cap`, `<sim_id>.containers.<name>.current_cap` and `.max_cap` |
| `add_variable(name, value)` | `<sim_id>.variables.<name>` |

Stores, registered through `SimParameter(stores=...)` on the low-level API, take `<sim_id>.stores.<name>.current_cap` and `.max_cap`. An update to a path that does not exist is logged as a warning and ignored.

---

## Building from YAML

`SimulationContext.from_yaml(path)` builds a context from a [YAML blueprint](yaml.md). The file is validated first, and every error names the file and line. The context is then built with the builder methods above, in the order a script calls them, so the result is an ordinary `SimulationContext`.

```python
app = SimulationContext.from_yaml("examples/yaml/local.yaml")
app.run()  # uses run.until from the file
```

`run()` calls the blueprint's `run.before` functions first, and uses `run.until` when no `until` is passed. The `dynamic-des run` command does the same from a shell.
