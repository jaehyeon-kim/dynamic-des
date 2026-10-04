"""YAML blueprints: validation, error lines and the builder calls they make."""

import textwrap

import pytest
from typer.testing import CliRunner

from dynamic_des import SimulationContext
from dynamic_des.blueprint import BlueprintError
from dynamic_des.blueprint.cli import app as cli
from dynamic_des.connectors.egress.base import BaseEgress

MINIMAL = """\
simulation:
  sim_id: Line_A
  factor: 0
  random_seed: 7
resources:
  lathe: {current_cap: 2, max_cap: 5}
services:
  milling: {dist: normal, mean: 3.0, std: 0.5}
arrivals:
  standard: {dist: exponential, rate: 1.0, spawn: part}
tasks:
  part:
    service: milling
    resource: lathe
    payload: {status: finished}
    id_field: part_id
run:
  until: 30
"""


def write(tmp_path, text, name="blueprint.yaml"):
    path = tmp_path / name
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


class _Capture(BaseEgress):
    """Keeps every record it receives, for assertions after the run."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        import asyncio
        import queue

        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def test_blueprint_builds_through_the_builder(tmp_path):
    app = SimulationContext.from_yaml(write(tmp_path, MINIMAL))

    assert (app.sim_id, app.factor, app.random_seed) == ("Line_A", 0, 7)
    assert app._resources_config["lathe"].current_cap == 2
    milling = app._services_config["milling"]
    assert (milling.dist, milling.mean, milling.std, milling.rate) == (
        "normal",
        3.0,
        0.5,
        0.0,
    )
    assert app._arrivals_config["standard"].rate == 1.0
    assert app._default_until == 30.0


def test_tasks_spawned_from_an_arrival_publish_the_payload(tmp_path):
    app = SimulationContext.from_yaml(write(tmp_path, MINIMAL))
    capture = _Capture()
    app.add_egress(capture)
    app.run()

    events = [r["value"] for r in capture.records if r["stream_type"] == "event"]
    finished = [value for value in events if "part_id" in value]
    assert finished
    assert finished[0] == {"status": "finished", "part_id": finished[0]["part_id"]}
    assert max(r["sim_ts"] for r in capture.records) <= 30.0


def test_telemetry_publishes_resource_statistics(tmp_path):
    text = MINIMAL + textwrap.dedent("""\
        telemetry:
          - interval: 5
            publish: {lathe.capacity: lathe.capacity, busy: lathe.utilization}
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=10)

    metrics = [
        (r["sim_ts"], r["path_id"], r["value"])
        for r in capture.records
        if r["stream_type"] == "telemetry" and r["path_id"].startswith("Line_A")
    ]
    assert metrics[:2] == [(0.0, "Line_A.lathe.capacity", 2), (0.0, "Line_A.busy", 0)]


def test_until_accepts_a_duration(tmp_path):
    app = SimulationContext.from_yaml(
        write(tmp_path, MINIMAL.replace("until: 30", "until: 2 min"))
    )
    assert app._default_until == 120.0


@pytest.mark.parametrize(
    "old,new,line,fragment",
    [
        ("  factor: 0\n", "  factor: 0\n  speed: 2\n", 4, "simulation.speed"),
        ("mean: 3.0, std: 0.5", "mean: 3.0, std: 0.5, shape: 2", 8, "shape"),
        ("dist: exponential", "dist: poisson", 10, "arrivals.standard.dist"),
        (
            "  lathe: {current_cap: 2, max_cap: 5}",
            "  lathe: {current_cap: 2}",
            6,
            "max_cap",
        ),
        ("until: 30", "until: soon", 18, "run.until"),
    ],
)
def test_schema_errors_name_the_line(tmp_path, old, new, line, fragment):
    path = write(tmp_path, MINIMAL.replace(old, new))
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert fragment in message


@pytest.mark.parametrize(
    "old,new,line,fragment",
    [
        ("    service: milling", "    service: drilling", 13, "service 'drilling'"),
        ("    resource: lathe", "    resource: press", 14, "resource 'press'"),
        ("spawn: part", "spawn: widget", 10, "task 'widget'"),
        ("current_cap: 2,", "current_cap: 2.5,", 6, "whole number"),
    ],
)
def test_cross_reference_errors_name_the_line(tmp_path, old, new, line, fragment):
    path = write(tmp_path, MINIMAL.replace(old, new))
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert fragment in message


def test_telemetry_reference_must_name_a_resource_and_stat(tmp_path):
    text = MINIMAL + "telemetry:\n  - interval: 1\n    publish: {x: lathe.speed}\n"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError, match=rf"{path}:21: 'lathe.speed'"):
        SimulationContext.from_yaml(path)


def test_unknown_connector_type_names_the_line(tmp_path):
    text = MINIMAL + "egress:\n  - type: Carrier\n"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError, match=rf"{path}:20: unknown connector type"):
        SimulationContext.from_yaml(path)


def test_bad_connector_config_names_the_line(tmp_path):
    text = MINIMAL + "egress:\n  - type: Console\n    config: {colour: red}\n"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError, match=rf"{path}:21: ConsoleEgress rejected"):
        SimulationContext.from_yaml(path)


def test_yaml_syntax_error_names_the_line(tmp_path):
    path = write(tmp_path, "simulation:\n  sim_id: [Line_A\n")
    with pytest.raises(BlueprintError, match=rf"{path}:\d+:"):
        SimulationContext.from_yaml(path)


def test_top_level_must_be_a_mapping(tmp_path):
    path = write(tmp_path, "- simulation\n")
    with pytest.raises(BlueprintError, match="top level must be a mapping"):
        SimulationContext.from_yaml(path)


def test_cli_runs_a_blueprint(tmp_path):
    path = write(tmp_path, MINIMAL + "egress:\n  - type: Console\n")
    result = CliRunner().invoke(cli, ["run", str(path), "--until", "5"])

    assert result.exit_code == 0, result.output


def test_cli_reports_an_invalid_blueprint(tmp_path):
    path = write(tmp_path, MINIMAL.replace("spawn: part", "spawn: widget"))
    result = CliRunner().invoke(cli, ["run", str(path)])

    assert result.exit_code == 1
    assert f"{path}:10:" in result.output


def test_cli_rejects_a_bad_until(tmp_path):
    path = write(tmp_path, MINIMAL)
    result = CliRunner().invoke(cli, ["run", str(path), "--until", "soon"])

    assert result.exit_code == 2
    assert "Invalid time format" in result.output


def test_cli_rejects_a_missing_file(tmp_path):
    result = CliRunner().invoke(cli, ["run", str(tmp_path / "absent.yaml")])

    assert result.exit_code == 2


def test_cli_prints_the_version():
    from dynamic_des import __version__

    result = CliRunner().invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() == __version__
