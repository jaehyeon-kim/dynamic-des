# Registry and Live Parameters

Every run has one `SimulationRegistry`, at `env.registry`. It maps dot-notation paths, such as `Line_A.resources.lathe.current_cap`, to the values the simulation reads. An ingress connector, a scenario or a process changes a parameter by updating its path, and the simulation follows the change without restarting.

---

## Registry Paths

`run()` flattens the configuration into the registry, and every path an ingress message, a scenario or `registry.get` names has one of these forms:

| Builder call | Paths |
|---|---|
| `add_arrival(name, ...)`, `add_service(name, ...)` | `<sim_id>.arrival.<name>.rate` and `<sim_id>.service.<name>.rate` for an exponential distribution, otherwise `.mean` and `.std` |
| `add_resource(name, ...)`, `add_container(name, ...)` | `<sim_id>.resources.<name>.current_cap` and `.max_cap`, `<sim_id>.containers.<name>.current_cap` and `.max_cap` |
| `add_variable(name, value)` | `<sim_id>.variables.<name>` |

Stores, registered through `SimParameter(stores=...)` on the low-level API, take `<sim_id>.stores.<name>.current_cap` and `.max_cap`. An update to a path that does not exist is logged as a warning and ignored.

`compile_parameters()` on a `SimulationContext` returns the `SimParameter` that `run()` registers, so the paths of a configuration can be checked before the run.

---

## How an Update Is Applied

`env.registry.update(path, value)` applies one change:

* A path that does not exist is logged as a warning, and the update is ignored.
* A value of a different type is converted to the type the path holds, so the string `"5"` sent to an integer path becomes `5`. The conversion is Python's own, so `2.5` sent to an integer path becomes `2`. A value that cannot be converted is logged as an error, and the update is ignored.
* A value equal to the current one changes nothing.
* Otherwise the value is replaced, and the configuration object the path belongs to is updated too, such as the `rate` of a `DistributionConfig` or an entry of the variables.

Ingress connectors run on a background thread, so they do not call `update` themselves. Each puts a `(path, value)` pair on a queue, and a SimPy process started by `setup_ingress` applies everything queued every 0.1 simulation seconds. At `factor=1.0` an update therefore lands within 0.1 seconds of arriving. The simulation time it lands at depends on when it arrives, so a change that must happen at an exact simulation time is a scenario.

---

## What Follows a Change

* **Arrival and service distributions.** A `Sampler` reads the configuration object each time it draws, so the next draw uses the new value. `context.wait_for_arrival` draws when it is called, so the arrival already being waited for keeps its old gap. `@app.task` draws the service time once the resource is acquired, so a task still in the queue uses the new value.
* **Resource capacity.** A `DynamicResource` follows `current_cap`, rounded down to a whole number and limited to between 0 and the `max_cap` the registry holds. A change to `max_cap` applies `current_cap` again under the new limit. [Resources and Containers](resources.md#shrinking-safety-guarantee) describes what happens when the capacity shrinks below the number of tasks in progress.
* **Container capacity.** A `DynamicContainer` follows `current_cap`, fractions included even when the starting value is a whole number, limited to between 0 and the `max_cap` the registry holds. A change to `max_cap` applies `current_cap` again under the new limit. A `DynamicStore` does the same with whole numbers.
* **Variables.** The new value is stored. A process reads it with `env.registry.get(path).value`.

---

## Sources of Updates

* **Ingress connectors**: Kafka, Redis, PostgreSQL and `LocalIngress`. [Connectors](connectors.md#1-ingress-connectors-inputs) gives the message each one expects.
* **A scenario** in a YAML blueprint, compared with `LocalIngress` below.
* **A process**, which calls `env.registry.update(path, value)` directly, as the maintenance process in [Advanced YAML](../guides/yaml-advanced.md#2-processes-payloads-and-kwargs) does.

### Scenarios versus `LocalIngress`
A [YAML blueprint](yaml.md) can carry a `scenario`: a list of registry changes, each with the simulation time it applies at, such as `{at: 10, path: Line_A.resources.lathe.current_cap, value: 3}`. [Script an experiment](../guides/yaml-blueprints.md#2-script-an-experiment) shows a complete file.

A scenario is not a connector. It is compiled into a SimPy process that waits on the simulation clock, so each change lands at exactly its `at`, on every run and at any `factor`, including `factor=0`. Every path is checked against the registry when the file is loaded, so a misspelt path stops the load with its line number. `LocalIngress` waits on the wall clock instead, and an unknown path is only logged as a warning when it is due.

A scenario and an ingress connector can be used together. Both write to the same registry, so a scripted baseline can run while an operator steers over Kafka. When both change the same path, the later write wins.

---

## Waiting for a Change

`env.registry.get(path).wait_for_change()` returns a SimPy event that fires when the value at that path changes. Each value has one change signal, and the first process waiting takes it. `DynamicResource`, `DynamicContainer` and `DynamicStore` already wait on their `current_cap` and `max_cap`, so wait only on paths that nothing else waits on, such as variables.
