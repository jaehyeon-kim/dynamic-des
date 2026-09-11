# Examples

Runnable scripts for every connector, in both API styles. They live outside the package, so `pip install dynamic-des` does not carry them and the library declares only what it imports.

Run one directly:

```bash
uv run examples/declarative/local_example.py
```

## What to install

From a clone, with uv, `--extra` installs what a script needs for that run:

```bash
uv run --extra kafka examples/declarative/kafka_example.py
```

With pip, install first and run with `python`:

```bash
pip install "dynamic-des[kafka]"
python examples/declarative/kafka_example.py
```

Extras by example: `kafka` for `*/kafka_example.py` and `kafka_dashboard.py`, `postgres` for `*/postgres_example.py`, `redis` for `*/redis_example.py`, `parquet` for `*/history_example.py`, both `kafka` and `parquet` for `declarative/backfill_live_example.py`, and none for `*/local_example.py`.

`kafka_dashboard.py` additionally needs `nicegui`, which is not a dependency of the library because nothing in it imports the package. Run it as `uv run --extra kafka --with nicegui examples/kafka_dashboard.py`, which installs nothing permanently, or `pip install nicegui` first.

## What to start

Containers come from [odctl](https://github.com/jaehyeon-kim/odctl), installed once with `uv tool install "odctl>=0.5.1"`. The floor matters: earlier versions created the Valkey user without a channel grant, so subscribing failed.

| Example | Profile | Start |
|---|---|---|
| `*/local_example.py` | none | |
| `*/kafka_example.py` | `kafka-lite` | `odctl up kafka-lite` |
| `declarative/backfill_live_example.py` | `kafka-lite` | `odctl up kafka-lite` |
| `kafka_dashboard.py` | `kafka-lite` | `odctl up kafka-lite` |
| `*/postgres_example.py` | `postgres` | `odctl up postgres` |
| `*/redis_example.py` | `valkey` | `odctl up valkey` |
| `*/history_example.py` | none, or `storage` with `USE_S3=true` | `odctl up storage` |

Stop a profile with `odctl down <profile> --volumes`. `odctl list -d` shows every profile and the ports it publishes.

Kafka and Redis are the two whose profile names are not what you would guess, because odctl ships a one-broker Kafka as `kafka-lite` and uses Valkey rather than Redis.

## Two API styles, same simulation

`declarative/` uses `SimulationContext`, the builder, where infrastructure is declared by chaining and the simulation logic hangs off decorators.

`imperative/` uses `DynamicRealtimeEnvironment` directly, wiring the registry, resources and connectors by hand. It is the lower-level API the builder is written on.

Most pairs run the same simulation, so reading one against the other shows what the builder does for you. The local pair is the exception and the two differ on purpose: `declarative/local_example.py` runs `Factory_A` for 60 seconds with no ingress, while `imperative/local_example.py` runs `Line_A` for 30 seconds and uses `LocalIngress` to schedule two capacity changes.
