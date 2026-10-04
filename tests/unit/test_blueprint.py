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


# ---------------------------------------------------------------------------
# !python references (#16)
# ---------------------------------------------------------------------------
LOGIC = """\
import itertools

calls = []


def payload(task_id, context):
    return {"task": task_id, "at": context.env.now}


def ticker(context, step, label="tick"):
    for count in itertools.count():
        yield context.env.timeout(step)
        context.publish(label, count)


def not_a_generator(context):
    return None


def sample(context):
    context.publish("sampled", 1)


def keep_events(record):
    return record["stream_type"] == "event"


def setup():
    calls.append("setup")


class Sink:
    def __init__(self, label):
        self.label = label

    async def run(self, egress_queue):
        return None

"""


@pytest.fixture
def logic(tmp_path, request):
    """A module beside the blueprint, with a name unique to the test."""
    name = f"logic_{request.node.name.replace('[', '_').replace(']', '_')}"
    name = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
    (tmp_path / f"{name}.py").write_text(LOGIC, encoding="utf-8")
    return name


def test_python_payload_is_called_with_the_task_id_and_context(tmp_path, logic):
    text = MINIMAL.replace(
        "    payload: {status: finished}\n    id_field: part_id\n",
        f"    payload: !python {logic}.payload\n",
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=10)

    events = [r["value"] for r in capture.records if r["stream_type"] == "event"]
    finished = [value for value in events if "task" in value]
    assert finished and finished[0]["at"] > 0


def test_processes_take_kwargs(tmp_path, logic):
    text = MINIMAL + textwrap.dedent(f"""\
        processes:
          - function: !python {logic}.ticker
            kwargs: {{step: 2}}
          - function: !python {logic}.ticker
            kwargs: {{step: 4, label: slow}}
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=9)

    ticks = [
        (r["path_id"], r["sim_ts"])
        for r in capture.records
        if r["stream_type"] == "telemetry" and not r["path_id"].startswith("system")
    ]
    assert ("Line_A.tick", 2.0) in ticks and ("Line_A.tick", 8.0) in ticks
    assert ("Line_A.slow", 4.0) in ticks and ("Line_A.slow", 8.0) in ticks


def test_telemetry_takes_a_python_function(tmp_path, logic):
    text = MINIMAL + textwrap.dedent(f"""\
        telemetry:
          - interval: 5
            function: !python {logic}.sample
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=10)
    assert any(r.get("path_id") == "Line_A.sampled" for r in capture.records)


def test_egress_type_and_when_take_python_objects(tmp_path, logic):
    text = MINIMAL + textwrap.dedent(f"""\
        egress:
          - type: !python {logic}.Sink
            config: {{label: one}}
            when: !python {logic}.keep_events
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    provider = app._egress_providers[0]
    assert type(provider).__name__ == "Sink" and provider.label == "one"
    assert app._egress_predicates[0]({"stream_type": "event"}) is True


def test_run_before_is_called_before_the_run(tmp_path, logic):
    import importlib

    text = MINIMAL.replace(
        "run:\n  until: 30\n",
        f"run:\n  until: 1\n  before: [!python {logic}.setup]\n",
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    module = importlib.import_module(logic)
    assert module.calls == []
    app.run()
    assert module.calls == ["setup"]


def test_the_blueprint_folder_is_on_sys_path(tmp_path, logic, monkeypatch):
    """A blueprint resolves the module beside it from any working directory."""
    import sys

    monkeypatch.chdir("/")
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p != str(tmp_path)])
    text = (
        MINIMAL + f"telemetry:\n  - interval: 1\n    function: !python {logic}.sample\n"
    )
    SimulationContext.from_yaml(write(tmp_path, text))
    assert sys.path[0] == str(tmp_path.resolve())


@pytest.mark.parametrize(
    "reference,fragment",
    [
        ("no_such_module_xyz.func", "no module named 'no_such_module_xyz'"),
        ("{logic}.missing", "has no attribute 'missing'"),
        ("{logic}", "expected a dotted path"),
        ("{logic}.not valid", "expected a dotted path"),
    ],
)
def test_python_reference_failures_name_the_line(tmp_path, logic, reference, fragment):
    text = MINIMAL + f"processes:\n  - !python {reference.format(logic=logic)}\n"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:20: !python"), message
    assert fragment in message


def test_python_reference_reports_an_import_failure_inside_the_module(tmp_path):
    (tmp_path / "logic_fails_on_import.py").write_text(
        "import no_such_dependency_xyz\n", encoding="utf-8"
    )
    path = write(
        tmp_path, MINIMAL + "processes:\n  - !python logic_fails_on_import.run\n"
    )
    with pytest.raises(
        BlueprintError, match=r"importing 'logic_fails_on_import' failed: No module"
    ):
        SimulationContext.from_yaml(path)


def test_python_tag_on_a_mapping_is_rejected(tmp_path):
    path = write(tmp_path, MINIMAL + "processes:\n  - !python {a: b}\n")
    with pytest.raises(BlueprintError, match=rf"{path}:20: !python takes a dotted"):
        SimulationContext.from_yaml(path)


def test_a_process_must_be_a_generator_function(tmp_path, logic):
    text = MINIMAL + f"processes:\n  - !python {logic}.not_a_generator\n"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError, match=rf"{path}:20: .*generator function"):
        SimulationContext.from_yaml(path)


def test_telemetry_needs_publish_or_function(tmp_path, logic):
    text = MINIMAL + "telemetry:\n  - interval: 1\n"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError, match="give either publish or function"):
        SimulationContext.from_yaml(path)


def test_id_field_needs_a_mapping_payload(tmp_path, logic):
    text = MINIMAL.replace(
        "    payload: {status: finished}\n",
        f"    payload: !python {logic}.payload\n",
    )
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError, match="id_field applies only to a mapping"):
        SimulationContext.from_yaml(path)


def test_plain_safe_load_is_unaffected():
    """The tag is registered on the blueprint loader only."""
    import yaml

    with pytest.raises(yaml.constructor.ConstructorError):
        yaml.safe_load("x: !python os.system\n")


# ---------------------------------------------------------------------------
# Scenarios (#15)
# ---------------------------------------------------------------------------
SCENARIO = """\
scenario:
  - {at: 20, path: Line_A.resources.lathe.current_cap, value: 1}
  - {at: 10, path: Line_A.resources.lathe.current_cap, value: 4}
  - {at: 15 s, path: Line_A.arrival.standard.rate, value: 3}
  - {at: 0, path: Line_A.variables.mode, value: warm}
"""


def test_scenario_applies_on_simulation_time_at_factor_0(tmp_path):
    text = (
        MINIMAL
        + "variables:\n  mode: cold\n"
        + SCENARIO
        + textwrap.dedent("""\
        telemetry:
          - interval: 1
            publish: {cap: lathe.capacity}
        """)
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=25)

    capacity = {
        r["sim_ts"]: r["value"]
        for r in capture.records
        if r.get("path_id") == "Line_A.cap"
    }
    assert capacity[9.0] == 2
    assert capacity[11.0] == 4
    assert capacity[21.0] == 1

    registry = app.env.registry
    assert registry.get("Line_A.arrival.standard.rate").value == 3.0
    assert registry.get("Line_A.variables.mode").value == "warm"


def test_scenario_repeats_exactly(tmp_path):
    text = MINIMAL + SCENARIO.replace(
        "  - {at: 0, path: Line_A.variables.mode, value: warm}\n", ""
    )

    def run_once():
        app = SimulationContext.from_yaml(write(tmp_path, text))
        capture = _Capture()
        app.add_egress(capture)
        app.run(until=25)
        return [
            (r["sim_ts"], r.get("key"), r["value"])
            for r in capture.records
            if r["stream_type"] == "event"
        ]

    assert run_once() == run_once()


@pytest.mark.parametrize(
    "step,fragment",
    [
        (
            "{at: 5, path: Line_A.resources.press.current_cap, value: 1}",
            "not a registry",
        ),
        ("{at: 5, path: Line_A.arrival.standard.mean, value: 1}", "not a registry"),
        (
            "{at: 5, path: Line_B.resources.lathe.current_cap, value: 1}",
            "not a registry",
        ),
        (
            "{at: 5, path: Line_A.resources.lathe.current_cap, value: many}",
            "holds a int",
        ),
    ],
)
def test_scenario_paths_are_checked_before_the_run(tmp_path, step, fragment):
    path = write(tmp_path, MINIMAL + f"scenario:\n  - {step}\n")
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:20:"), message
    assert fragment in message


def test_scenario_at_must_be_a_time(tmp_path):
    path = write(
        tmp_path,
        MINIMAL + "scenario:\n  - {at: later, path: Line_A.variables.x, value: 1}\n",
    )
    with pytest.raises(BlueprintError, match=rf"{path}:20: scenario.0.at"):
        SimulationContext.from_yaml(path)


def test_compile_parameters_matches_what_run_registers(tmp_path):
    app = SimulationContext.from_yaml(write(tmp_path, MINIMAL))
    params = app.compile_parameters()
    app.run(until=1)

    assert params.sim_id == "Line_A"
    assert (
        app.env.registry.get_config("Line_A.resources.lathe")
        is (params.resources["lathe"])
    )


# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
ENV_BLUEPRINT = """\
simulation:
  sim_id: ${SIM_ID}
  factor: ${FACTOR:-0}
  random_seed: 7
variables:
  host: ${DB_HOST:-localhost}
  url: "postgres://${DB_HOST:-localhost}:${DB_PORT:-5432}/sim"
  port: ${DB_PORT:-5432}
  quoted_port: "${DB_PORT:-5432}"
  literal: $${NOT_A_VARIABLE}
run:
  until: ${UNTIL:-1 min}
"""


def test_environment_variables_are_substituted(tmp_path, monkeypatch):
    monkeypatch.setenv("SIM_ID", "Line_B")
    monkeypatch.setenv("DB_HOST", "db.internal")
    monkeypatch.delenv("FACTOR", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    monkeypatch.delenv("UNTIL", raising=False)
    monkeypatch.delenv("NOT_A_VARIABLE", raising=False)
    app = SimulationContext.from_yaml(write(tmp_path, ENV_BLUEPRINT))

    assert (app.sim_id, app.factor, app._default_until) == ("Line_B", 0, 60.0)
    variables = app._variables_config
    assert variables["host"] == "db.internal"
    assert variables["url"] == "postgres://db.internal:5432/sim"
    # An unquoted value is typed after substitution; a quoted one stays a string.
    assert variables["port"] == 5432
    assert variables["quoted_port"] == "5432"
    assert variables["literal"] == "${NOT_A_VARIABLE}"


def test_an_empty_variable_takes_the_default(tmp_path, monkeypatch):
    monkeypatch.setenv("SIM_ID", "Line_B")
    monkeypatch.setenv("DB_PORT", "")
    app = SimulationContext.from_yaml(write(tmp_path, ENV_BLUEPRINT))
    assert app._variables_config["port"] == 5432


def test_an_unset_variable_without_a_default_names_the_line(tmp_path, monkeypatch):
    monkeypatch.delenv("SIM_ID", raising=False)
    path = write(tmp_path, ENV_BLUEPRINT)
    with pytest.raises(
        BlueprintError, match=rf"{path}:2: environment variable SIM_ID is not set"
    ):
        SimulationContext.from_yaml(path)


def test_python_references_take_environment_variables(tmp_path, logic, monkeypatch):
    monkeypatch.setenv("LOGIC", logic)
    text = MINIMAL + "processes:\n  - function: !python ${LOGIC}.ticker\n"
    text += "    kwargs: {step: 2}\n"
    app = SimulationContext.from_yaml(write(tmp_path, text))
    assert app._startup_loops[-1][0].func.__name__ == "ticker"


# ---------------------------------------------------------------------------
# Relative times
# ---------------------------------------------------------------------------
def _with_times(start, live):
    return MINIMAL.replace(
        "  random_seed: 7\n",
        f"  random_seed: 7\n  logical_start_time: {start}\n  go_live_at: {live}\n",
    )


@pytest.mark.parametrize(
    "start,live,start_offset,live_offset",
    [
        ("-1d", "now", -86400, 0),
        ("-7d", "-10m", -7 * 86400, -600),
        ("-10 min", "+30s", -600, 30),
        ("now", "now", 0, 0),
    ],
)
def test_relative_times_read_one_now(tmp_path, start, live, start_offset, live_offset):
    from datetime import datetime, timedelta

    before = datetime.now()
    app = SimulationContext.from_yaml(write(tmp_path, _with_times(start, live)))
    after = datetime.now()

    assert app.go_live_at - app.logical_start_time == timedelta(
        seconds=live_offset - start_offset
    )
    now = app.logical_start_time - timedelta(seconds=start_offset)
    assert before <= now <= after


def test_absolute_times_are_read_as_datetimes(tmp_path):
    from datetime import datetime

    text = _with_times("2026-01-01T00:00:00", "'2026-01-02T06:30:00'")
    app = SimulationContext.from_yaml(write(tmp_path, text))
    assert app.logical_start_time == datetime(2026, 1, 1)
    assert app.go_live_at == datetime(2026, 1, 2, 6, 30)


def test_times_take_python_objects(tmp_path, logic):
    from datetime import datetime

    (tmp_path / f"{logic}_times.py").write_text(
        "from datetime import datetime\nSTART = datetime(2026, 3, 1)\n",
        encoding="utf-8",
    )
    text = _with_times(f"!python {logic}_times.START", "now")
    app = SimulationContext.from_yaml(write(tmp_path, text))
    assert app.logical_start_time == datetime(2026, 3, 1)


@pytest.mark.parametrize(
    "value,fragment", [("-1 fortnight", "not a duration"), ("yesterday", "not a time")]
)
def test_a_bad_time_names_the_line(tmp_path, value, fragment):
    path = write(tmp_path, _with_times(value, "now"))
    with pytest.raises(
        BlueprintError, match=rf"{path}:5: simulation.logical_start_time: .*{fragment}"
    ):
        SimulationContext.from_yaml(path)


# ---------------------------------------------------------------------------
# when: history and when: live
# ---------------------------------------------------------------------------
SPLIT = """\
egress:
  - type: Console
    when: history
  - type: Console
    when: live
telemetry:
  - interval: 1
    publish: {busy: lathe.in_use}
"""


def test_history_and_live_split_records_at_go_live(tmp_path):
    text = _with_times("2026-01-01T00:00:00", "2026-01-01T00:00:10") + SPLIT
    app = SimulationContext.from_yaml(write(tmp_path, text))
    history, live = _Capture(), _Capture()
    # The YAML providers are swapped for captures; the predicates stay as built.
    app._egress_providers[:] = [history, live]
    # Ten simulation seconds of history, then a short live tail in real time.
    app.run(until=10.3)

    assert history.records and live.records
    assert all(r["timestamp"] < "2026-01-01T00:00:10.000" for r in history.records)
    assert all(r["timestamp"] >= "2026-01-01T00:00:10.000" for r in live.records)
    assert max(r["sim_ts"] for r in history.records) < 10
    assert min(r["sim_ts"] for r in live.records) == 10


def test_live_is_compared_in_the_zone_of_the_start_time(tmp_path):
    text = _with_times("2026-01-01T00:00:00+00:00", "2026-01-01T10:00:10+10:00") + SPLIT
    app = SimulationContext.from_yaml(write(tmp_path, text))
    history, live = app._egress_predicates
    record = {"timestamp": "2026-01-01T00:00:10.000+00:00"}
    assert live(record) and not history(record)
    record = {"timestamp": "2026-01-01T00:00:09.999+00:00"}
    assert history(record) and not live(record)


def test_history_needs_go_live_at(tmp_path):
    path = write(tmp_path, MINIMAL + "egress:\n  - type: Console\n    when: history\n")
    with pytest.raises(
        BlueprintError, match=rf"{path}:21: when: history needs simulation.go_live_at"
    ):
        SimulationContext.from_yaml(path)


def test_an_unknown_when_names_the_line(tmp_path):
    path = write(tmp_path, MINIMAL + "egress:\n  - type: Console\n    when: later\n")
    with pytest.raises(
        BlueprintError,
        match=rf"{path}:21: egress.0.when: .*'later' is not a when. Use history, live",
    ):
        SimulationContext.from_yaml(path)


# ---------------------------------------------------------------------------
# Tasks with no service and no resource
# ---------------------------------------------------------------------------
def test_a_task_without_service_or_resource_emits_at_once(tmp_path):
    text = MINIMAL.replace("    service: milling\n    resource: lathe\n", "")
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=10)

    events = [r for r in capture.records if r["stream_type"] == "event"]
    assert events
    # Only the payload: no queued or started event, and no time in service.
    assert all(set(r["value"]) == {"status", "part_id"} for r in events)
    assert [r["value"]["part_id"] for r in events] == list(range(len(events)))


@pytest.mark.parametrize("drop", ["    service: milling\n", "    resource: lathe\n"])
def test_a_task_needs_service_and_resource_together(tmp_path, drop):
    path = write(tmp_path, MINIMAL.replace(drop, ""))
    with pytest.raises(
        BlueprintError, match=rf"{path}:12: tasks.part: .*give both service and"
    ):
        SimulationContext.from_yaml(path)


def test_telemetry_publishes_capacity_and_in_use(tmp_path):
    text = MINIMAL + textwrap.dedent("""\
        telemetry:
          - interval: 1
            publish:
              cap: lathe.capacity
              busy: lathe.in_use
              util: lathe.utilization
              queue: lathe.queue_length
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=20)

    samples = {}
    for r in capture.records:
        if r["stream_type"] == "telemetry" and r["path_id"].startswith("Line_A."):
            samples.setdefault(r["sim_ts"], {})[r["path_id"][7:]] = r["value"]
    assert len(samples) == 20
    assert all(s["cap"] == 2 for s in samples.values())
    assert all(s["busy"] / s["cap"] * 100 == s["util"] for s in samples.values())
    assert any(s["busy"] > 0 for s in samples.values())


# ---------------------------------------------------------------------------
# Containers, positive settings, connector errors and time zones
# ---------------------------------------------------------------------------
def test_container_capacity_keeps_fractions(tmp_path):
    text = MINIMAL + textwrap.dedent("""\
        containers:
          tank: {current_cap: 50.5, max_cap: 100}
        scenario:
          - {at: 5, path: Line_A.containers.tank.current_cap, value: 62.5}
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    app.run(until=10)

    assert app.env.registry.get("Line_A.containers.tank.current_cap").value == 62.5
    assert app.env.registry.get("Line_A.containers.tank.max_cap").value == 100


@pytest.mark.parametrize(
    "extra,line,fragment",
    [
        (
            "telemetry:\n  - interval: 0\n    publish: {cap: lathe.capacity}\n",
            20,
            "interval",
        ),
        ("batching: {batch_size: 0, flush_interval: 1}\n", 19, "batch_size"),
        ("batching: {batch_size: 10, flush_interval: 0}\n", 19, "flush_interval"),
        ("egress:\n  - type: Console\n    flush_interval: 0\n", 21, "flush_interval"),
    ],
)
def test_zero_intervals_and_sizes_are_rejected(tmp_path, extra, line, fragment):
    path = write(tmp_path, MINIMAL + extra)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert fragment in message


def test_zero_until_is_rejected(tmp_path):
    path = write(tmp_path, MINIMAL.replace("until: 30", "until: 0"))
    with pytest.raises(BlueprintError, match=rf"{path}:18: .*run.until"):
        SimulationContext.from_yaml(path)


def test_a_connector_value_error_names_the_line(tmp_path):
    pytest.importorskip("pyiceberg")
    text = MINIMAL + textwrap.dedent("""\
        egress:
          - type: Iceberg
            config:
              catalog: {type: rest, uri: "http://localhost:8181"}
        """)
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:21:"), message
    assert "IcebergStorageEgress rejected its config" in message


@pytest.mark.parametrize(
    "times",
    [
        "  go_live_at: 2026-01-01T00:00:10+00:00\n",
        "  logical_start_time: 2026-01-01T00:00:00\n"
        "  go_live_at: 2026-01-01T00:00:10+00:00\n",
        "  logical_start_time: 2026-01-01T00:00:00+00:00\n"
        "  go_live_at: 2026-01-01T00:00:10\n",
    ],
)
def test_mixed_time_zones_name_the_line(tmp_path, times):
    path = write(
        tmp_path, MINIMAL.replace("  random_seed: 7\n", "  random_seed: 7\n" + times)
    )
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:"), message
    assert "both have a time zone or both have none" in message
