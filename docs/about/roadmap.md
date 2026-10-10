# Roadmap

*(Last updated: October 2026)*

This page lists the work planned for Dynamic DES. It changes as plans change, and nothing on it is a promise of a date or a release.

## Planned for 1.0

### Simulation model

- **Checked parameters:** a wrong value is refused when a model loads or when it changes during a run, instead of being silently replaced or crashing the run.
- **More distributions,** including one built from values taken from real data.
- **Routing between tasks:** a finished task leads to the next one, chosen by a condition or by probability, so a model can be a multi-step flow.
- **Entity state:** an order, a session or a part keeps its attributes as it moves from task to task, and every event of one entity carries its key.
- **Delays without a resource,** for steps that only take time.
- **Stores and containers** used by tasks, with a choice of queue order and task priority.
- **Built-in statistics:** wait and service times, throughput, utilisation and queue length, published as telemetry.
- **Ground-truth records:** a scenario step that injects a fault can publish a record of what it changed, sent only to chosen sinks, so a detector or an agent can be scored against it.
- **`ddes explain`:** shows a simulation as a graph, says whether each part is stable, points to the bottleneck and suggests changes.

### Simulation server

A server, started with `ddes serve`, runs simulations for you. It ships in three steps, and each one is usable on its own:

1. **API.** Upload a simulation, start and stop runs, change parameters while a run is going and follow its events, all through a REST API.
2. **UI.** Do the same from a browser, including each simulation's graph from `ddes explain`.
3. **Scheduler.** Run simulations on cron schedules.

A published server image and a profile for [odctl](https://github.com/jaehyeon-kim/odctl) then run the server locally with nothing installed on the host.

### Connectors and platform

- **More storage:** Parquet and JSONL on more cloud storage, and more Iceberg catalogs.
- **Speed:** faster runs without changing the published records.
- **Python 3.15** support.

### Docs

- **Concepts:** the simulation building blocks, the queueing behind them, and the distributions.
- **Tutorials:** from an idea to a stable model with `ddes explain`, and running on the server.
- **Reference:** the YAML format, the command line and the record format in one place.
- **`llms.txt`:** an index of these docs for AI assistants.

## What 1.0 means

From 1.0, the YAML blueprint format, the `ddes` command, the public Python API (`SimulationContext`, `DynamicRealtimeEnvironment` and the connectors) and the published record format stay compatible across every 1.x release. The package status moves from Beta to Production/Stable.

## Future work

### Simulation model

- **Breakdowns, schedules and incidents:** failures and repairs, planned maintenance, arrival rates and shifts by time of day, and faults that start at random times.
- **More flow and resource features:** preemption, batching, matching and splitting, reneging, setup times, and resources chosen by skill.
- **Reuse, costs and continuous flow:** parts of a model defined once and used several times, costs and energy per resource state, and levels that rise and fall continuously.
- **Transport on maps:** vehicles that move between locations on real roads and publish their positions.
- **Data-driven inputs:** arrivals replayed from a file or a database, or taken live from a Kafka topic.
- **Decisions from outside:** a dispatcher or an agent answers for one order or session, and a run can repeat exactly from a log of those answers.
- **Trustworthy results:** replications with confidence intervals, a warm-up period, and separate random streams so two versions of a model can be compared fairly.

### Analysis

- **Experiments:** parameter sweeps, experiment designs, optimisation, and calibration against observed data.
- **Fitting and comparison:** fit distributions to real data, and compare runs and variants.

### Simulation server

- **Visualisation:** animation of a running or recorded simulation, and a map view.
- **Distributed runs:** workers on several machines.

### Writing simulations

- **Generated values in blueprints:** random values, IDs and timestamps in YAML, then entities, relations between records, states and ramps.
- **Preview loop:** print a few sample records from a blueprint, run it again each time the file changes, and check a file without running it.
- **Example gallery and visual builder.**
- **AI assistants and agents:** a skill that teaches an assistant to write blueprints, and tools that let an agent run and steer simulations.

### Connectors

- **More sinks:** a webhook sink, a generic SQL sink and more.
- **Delivery faults:** records delivered late, twice, damaged or not at all, to test how a pipeline handles them.
- **Lineage:** each run reported to OpenLineage and OpenMetadata.

### Platform

- **More speed:** larger changes for faster runs, and a faster runtime for YAML-only blueprints if measurements show the need.

### Docs

- **Reference:** a page for each generator and connector, with examples and the records they produce, and a command-line cheat sheet.
- **Guides:** generating data at scale, testing pipelines with simulated data, and performance tuning.
