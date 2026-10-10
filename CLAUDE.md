# dynamic-des

Real-time SimPy simulations whose parameters change while they run, writing events and telemetry to Kafka, Redis, PostgreSQL, Parquet and JSONL files, and Iceberg tables. The public roadmap is `docs/about/roadmap.md`.

## Layout

| Path | Contents |
|---|---|
| `src/dynamic_des/core/` | `environment.py` (`DynamicRealtimeEnvironment`, egress buffering, record building), `context.py` (the `SimulationContext` builder), `registry.py` (live parameters), `sampler.py` (distributions) |
| `src/dynamic_des/models/` | `params.py` (`SimParameter`, `DistributionConfig`, `CapacityConfig`), `schemas.py` (`EventPayload`, `TelemetryPayload`: the published record schema) |
| `src/dynamic_des/resources/` | `DynamicResource`, `DynamicContainer`, `DynamicStore`, with capacities driven by the registry |
| `src/dynamic_des/connectors/` | `ingress/` (parameter changes in), `egress/` (records out), `admin/` |
| `src/dynamic_des/blueprint/` | YAML blueprints: `models.py` (Pydantic models), `loader.py` (`!python`), `build.py`, `cli.py` (`ddes`) |
| `examples/` | `imperative/` (low-level API), `declarative/` (builder), `yaml/` (blueprints) |
| `docs/` | MkDocs Material site, versioned with mike, deployed from `main` |
| `tests/unit/`, `tests/integration/` | unit tests; integration tests against containers started with odctl |
| `benchmarks/` | `throughput.py` (see issue #24) |

## Commands

```bash
uv run --all-extras pytest tests/unit -q          # unit tests, as CI runs them (--all-extras matters)
uv run --all-extras pytest tests/integration -ra  # needs Docker; starts odctl profiles on demand
uvx pre-commit run --all-files                    # ruff, ruff format, mypy, whitespace, YAML
DISABLE_MKDOCS_2_WARNING=true uv run mkdocs build --strict
uv run mkdocs serve -a 127.0.0.1:8001             # docs at http://127.0.0.1:8001/dynamic-des/
```

A plain `uv run` without `--all-extras` removes the extras from the venv, and the connector tests then fail to import.

## Rules

- **Three APIs.** A feature works in the low-level API, the builder and YAML wherever it can, with an example in each of `examples/imperative/`, `examples/declarative/` and `examples/yaml/`.
- **Docs with every feature.** A feature PR carries its docs pages, examples and nav entries. A feature without docs does not merge.
- **Docs code blocks are copies of files.** A Python or YAML block labelled `title="examples/..."` or `title="docs/snippets/..."` must match that file exactly; `tests/unit/test_docs_match_examples.py` checks it. Change the file, then copy it into the page again. Every YAML block must be labelled with a file.
- **Record contract.** `publish_event` and `publish_telemetry` build plain dicts with the same keys and order as `EventPayload` and `TelemetryPayload`. Changing a field, its type or the `timestamp` format breaks downstream consumers and needs an upgrade note.
- **Extras.** Each connector's dependencies are in an extra (`kafka`, `confluent`, `glue`, `redis`, `postgres`, `parquet`, `iceberg`, `all`). The `extras-isolation` CI job installs each extra alone, so import optional packages lazily.
- **Infrastructure comes from odctl** (`odctl>=1.0,<2` in the dev group): `kafka-lite`, `postgres`, `valkey`, `storage`, `catalog`.
- **Python modules.** Shared settings in `config.py`, models in `models.py`, names used only inside a module start with `_`.
- **Writing.** Plain English for non-native readers. No em or en dashes. Headings do not start with "The". Markdown paragraphs are one line each, never hard-wrapped.
- **ShadowTraffic** may be named as inspiration in a sentence, but nothing is copied from its docs or examples, and its name never appears in a package, command, module or page title.

## Release

1. Bump `version` in `pyproject.toml`; merging to `main` deploys the docs as that version, aliased `latest`, and copies its `404.html` to the site root.
2. Tag `vX.Y.Z` and publish a GitHub release. `publish.yml` runs on the published release and uploads to PyPI.
3. Python 3.15 is not in the CI matrix yet: confluent-kafka has no 3.15 wheel (issue #40).

## Git

- Commit or push only when asked. Do not create branches unless asked.
- Merge a bot PR with the owner as author: `gh pr merge <n> --squash --author-email <maintainer email>`. A plain squash merge adds the bot to the contributors list.
