# Roadmap

This page lists what is planned for Dynamic DES. It names no dates. Version numbers are set when a release ships.

## Next: simulation server

A server, started with `ddes serve`, runs simulations for you. It ships in three releases, and each one is usable on its own:

1. **API.** Upload a simulation, start and stop runs, change parameters while a run is going and follow its events, all through a REST API.
2. **UI and odctl.** Do the same from a browser. An odctl profile, started with `odctl up ddes`, runs the server with nothing installed on the host.
3. **Scheduler.** Run simulations on cron schedules.

## 1.0: stable release

1.0 follows the server. From 1.0, the YAML blueprint format, the `ddes` command and the public Python API (`SimulationContext`, `DynamicRealtimeEnvironment` and the connectors) stay compatible across every 1.x release. The package status moves from Beta to Production/Stable.

## After 1.0

These features are planned in no fixed order. Each one is built when a project needs it:

- **Generated values in blueprints:** random values, IDs and timestamps in YAML, then entities, relations between records, states and ramps.
- **Preview loop:** print a few sample records from a blueprint, run it again each time the file changes, and check a file without running it.
- **Delivery faults:** records delivered late, twice or not at all, to test how a pipeline handles them.
- **More storage:** Parquet and JSONL on GCS and Azure, and more Iceberg catalogs ([#32](https://github.com/jaehyeon-kim/dynamic-des/issues/32)).
- **More sinks:** a webhook sink and a generic SQL sink.
