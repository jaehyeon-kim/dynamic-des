"""Builds a `SimulationContext` from a YAML blueprint file.

Use `SimulationContext.from_yaml` from Python, or `ddes run <file>.yaml` from a
shell.
"""

from dynamic_des.blueprint.build import build
from dynamic_des.blueprint.loader import BlueprintError

__all__ = ["BlueprintError", "build"]
