"""The low-level tutorial shows a script that runs, in pieces that come from it.

The page walks through the script one function at a time, then shows the whole file.
test_docs_match_examples.py checks the whole file against the page. This checks that
every piece shown before it is part of that file, and that the script runs and
publishes the events the page describes. The YAML tutorial's blueprint is under
docs/snippets/yaml/, where test_docs_snippets.py builds and runs it.
"""

import importlib.util
import logging
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "docs" / "tutorials" / "low-level.md"
SCRIPT = ROOT / "docs" / "snippets" / "tutorials" / "first_factory_low_level.py"
# Python blocks with no title: the pieces shown before the full script.
PIECE = re.compile(r"^```python\n(.*?)\n```$", re.S | re.M)


def test_every_piece_is_part_of_the_script():
    pieces = PIECE.findall(PAGE.read_text(encoding="utf-8"))
    assert len(pieces) >= 5, "a regex that matches nothing would pass vacuously"
    source = SCRIPT.read_text(encoding="utf-8")
    missing = [piece.splitlines()[0] for piece in pieces if piece not in source]
    assert missing == [], f"{PAGE.name} shows code that is not in {SCRIPT.name}"


def test_script_runs_and_publishes_part_events(caplog):
    spec = importlib.util.spec_from_file_location("first_factory_low_level", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    with caplog.at_level(logging.INFO, logger="dynamic_des"):
        module.run(until=30, factor=0.0)

    events = [r.getMessage() for r in caplog.records if "[EVT]" in r.getMessage()]
    assert any("'status': 'queued'" in line for line in events)
    assert any("'status': 'started'" in line for line in events)
    assert any("'value': {'part_id': 0}" in line for line in events)
