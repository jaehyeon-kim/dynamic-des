"""Every blueprint shown in the documentation builds.

The snippets under docs/snippets/yaml/ are complete files, so each one is loaded and
run for a short time at factor 0. The hot rolling snippet references modules from
another repository and is checked only as YAML.
"""

import sys
from pathlib import Path

import pytest
import yaml

from dynamic_des import SimulationContext
from dynamic_des.blueprint.loader import BlueprintLoader

ROOT = Path(__file__).resolve().parents[2]
SNIPPETS = sorted((ROOT / "docs" / "snippets" / "yaml").glob("*.yaml"))


def test_snippets_exist():
    assert len(SNIPPETS) >= 3


@pytest.mark.parametrize("path", SNIPPETS, ids=[p.name for p in SNIPPETS])
def test_snippet_builds_and_runs(path):
    sys.modules.pop("workshop_logic", None)
    app = SimulationContext.from_yaml(path)
    app.factor = 0.0
    app.run(until=200)


def test_hot_rolling_snippet_is_valid_yaml():
    """Composed, not constructed: its !python references live in the OML repository."""
    path = ROOT / "docs" / "snippets" / "hot_rolling" / "hot_rolling.yaml"
    node = yaml.compose(path.read_text(encoding="utf-8"), Loader=BlueprintLoader)
    assert node is not None
