# Ways to Write a Simulation

Dynamic DES provides three ways to build your event-driven simulations, allowing you to choose between ease of use and raw control. A YAML blueprint holds only configuration, the Standard API adds Python logic through a builder, and the Low-Level API gives raw control.

---

## Three Ways

| Feature | YAML Blueprint | Standard API (Declarative) | Low-Level API (Imperative) |
|---|---|---|---|
| **Entry Point** | `ddes run` or `SimulationContext.from_yaml` | `SimulationContext` | `DynamicRealtimeEnvironment` |
| **Philosophy** | Declare the configuration in a file, and reference Python for the logic. | Define *what* the system looks like and use decorators for task lifecycles. | Define *how* every event and resource operates step-by-step. |
| **Boilerplate** | None for configuration. Logic is Python referenced with `!python`. | Low (Automatic event emission, resource requesting, and sampling). | High (Manual queueing, starting, timing out, and releasing). |
| **Typical Use Case** | Varying parameters, connectors and timed experiments between runs without editing code. | Building standard digital twins, historical data generation, and forecasting pipelines. | Edge-case scenarios requiring dynamic topology changes mid-run. |

The three are layers, not alternatives. A blueprint is built through the Standard API's builder methods, and the builder runs on the Low-Level API, so a blueprint can reference Python written for the builder, and a builder process can use the environment directly.

### Which to choose

* **YAML Blueprint** when the configuration is what changes between runs: rates, capacities, connectors, batching or a scripted experiment. The file is easy to review and diff, and it has a built-in scenario of timed changes on the simulation clock. Logic stays in a Python module beside the file.
* **Standard API** when the logic is most of the program and you want it in one Python file, or when you build the configuration in code, for example from a loop.
* **Low-Level API** when you need what the builder does not do, such as resources created mid-run or full control of every event.

---

## Same Parameters, Same Environment

All three end in the same two objects: a `SimParameter` holding the parameters, and a `DynamicRealtimeEnvironment` that runs them.

```text
YAML blueprint ──(from_yaml makes the builder calls)──> SimulationContext
SimulationContext ──(run() compiles the builder state)──> SimParameter
Low-level script ──(builds it by hand)──────────────────> SimParameter
SimParameter ──(registry.register_sim_parameter)──> DynamicRealtimeEnvironment
```

* **Low-level API**: the script builds the `SimParameter` and registers it with `env.registry.register_sim_parameter`.
* **Declarative API**: each builder method adds to the parameters, and `compile_parameters()` returns them as a `SimParameter`. `run()` creates the environment, registers that `SimParameter`, and then starts the processes.
* **YAML blueprint**: `SimulationContext.from_yaml` makes the builder calls a script would make, so the result is an ordinary `SimulationContext`.

Everything after that point is shared. The registry paths, the records each run publishes, the batching and the connectors are the same whichever way the simulation was written. The pages under Runtime describe them once for all three.

Each way has its own page:

* [Low-level API](low-level.md): the environment, the registry and SimPy processes, written by hand.
* [Declarative API](context.md): the `SimulationContext` builder and its decorators.
* [YAML Blueprints](yaml.md): every section of a blueprint file.
