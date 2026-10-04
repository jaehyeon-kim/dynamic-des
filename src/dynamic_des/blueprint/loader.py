"""Reads a blueprint file and remembers the line every key came from."""

from pathlib import Path
from typing import Any, Dict, Iterable, Tuple, Union

import yaml

Location = Tuple[Union[str, int], ...]


class BlueprintError(ValueError):
    """A blueprint that cannot be built. The message names the file and line."""


class BlueprintLoader(yaml.SafeLoader):
    """SafeLoader for blueprint files.

    It constructs only plain YAML types, as `yaml.safe_load` does.
    """


class SourceMap:
    """Maps a location in the document, such as `("tasks", "mill", "service")`, to a line.

    Validation errors and cross-reference errors carry a location rather than a line,
    so this is what turns them into `file:line` messages a reader can act on.
    """

    def __init__(self, path: Path, lines: Dict[Location, int]):
        self.path = path
        self._lines = lines

    def line(self, location: Iterable[Union[str, int]]) -> int:
        """Line of the deepest part of `location` the document has, or 1."""
        location = tuple(location)
        for end in range(len(location), -1, -1):
            if location[:end] in self._lines:
                return self._lines[location[:end]]
        return 1

    def error(
        self, location: Iterable[Union[str, int]], message: str
    ) -> BlueprintError:
        """Builds a `BlueprintError` pointing at `location`."""
        return BlueprintError(f"{self.path}:{self.line(location)}: {message}")


def _record_lines(node: yaml.Node, location: Location, lines: Dict[Location, int]):
    """Walks the node tree and records the line of every key and list item."""
    lines.setdefault(location, node.start_mark.line + 1)
    if isinstance(node, yaml.MappingNode):
        for key_node, value_node in node.value:
            child = location + (key_node.value,)
            # A key's own line is where a reader looks, even when its value is a
            # block starting on the next line.
            lines[child] = key_node.start_mark.line + 1
            _record_lines(value_node, child, lines)
    elif isinstance(node, yaml.SequenceNode):
        for index, item in enumerate(node.value):
            _record_lines(item, location + (index,), lines)


def read_document(path: Union[str, Path]) -> Tuple[Dict[str, Any], SourceMap]:
    """Parses a blueprint file into a mapping and its source map.

    Raises:
        BlueprintError: If the file is not valid YAML or is not a mapping.
    """
    path = Path(path)
    loader = BlueprintLoader(path.read_text(encoding="utf-8"))
    loader.name = str(path)
    lines: Dict[Location, int] = {}
    try:
        node = loader.get_single_node()
        if node is None:
            raise BlueprintError(f"{path}:1: the file is empty")
        _record_lines(node, (), lines)
        data = loader.construct_document(node)
    except yaml.MarkedYAMLError as exc:
        mark = exc.problem_mark or exc.context_mark
        line = mark.line + 1 if mark else 1
        raise BlueprintError(f"{path}:{line}: {exc.problem or exc}") from None
    finally:
        loader.dispose()

    source = SourceMap(path, lines)
    if not isinstance(data, dict):
        raise source.error((), "the top level must be a mapping of sections")
    return data, source
