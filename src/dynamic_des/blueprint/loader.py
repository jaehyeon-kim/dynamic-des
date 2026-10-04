"""Reads a blueprint file and remembers the line every key came from.

`${VAR}` and `${VAR:-default}` in a string are replaced with the environment
variable, so one file can serve several environments. The default applies when the
variable is unset or empty, as in a shell. `$${` writes a literal `${`. An unquoted
value is then read as YAML reads it, so `port: ${PG_PORT:-5432}` gives the number
5432. A quoted value stays a string.

`!python module.attribute` imports the module and returns the attribute, so a
blueprint can reference a process, a router or any other Python object. Loading a
blueprint therefore runs the imports it names. Every other node is constructed by
`yaml.SafeLoader`, as `yaml.safe_load` would.
"""

import importlib
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple, Union

import yaml

Location = Tuple[Union[str, int], ...]


class BlueprintError(ValueError):
    """A blueprint that cannot be built. The message names the file and line."""


class BlueprintLoader(yaml.SafeLoader):
    """SafeLoader for blueprint files, with `${VAR}` substitution and one extra tag.

    The `!python` tag and the substitution are registered on this subclass only, so
    `yaml.safe_load` elsewhere in the process is unaffected.
    """


_VARIABLE = re.compile(r"\$\$\{|\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def substitute(text: str) -> str:
    """Replaces `${VAR}` and `${VAR:-default}` with the environment variable.

    `${VAR:-default}` uses the default when the variable is unset or empty. `$${` is
    a literal `${`.

    Raises:
        ValueError: If a variable without a default is unset.
    """

    def replace(match: "re.Match[str]") -> str:
        if match.group(0) == "$${":
            return "${"
        name, default = match.group(1), match.group(2)
        value = os.environ.get(name)
        if value:
            return value
        if default is not None:
            return default
        if value is not None:
            return value
        raise ValueError(
            f"environment variable {name} is not set. Set it, or give a default "
            f"with ${{{name}:-default}}"
        )

    return _VARIABLE.sub(replace, text)


def _substitute_at(loader: yaml.SafeLoader, node: yaml.Node, text: str) -> str:
    """`substitute`, with a failure reported at the line of `node`."""
    try:
        return substitute(text)
    except ValueError as exc:
        raise BlueprintError(
            f"{loader.name}:{node.start_mark.line + 1}: {exc}"
        ) from None


def _construct_str(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> Any:
    text = loader.construct_scalar(node)
    if "${" not in text:
        return text
    text = _substitute_at(loader, node, text)
    # An unquoted value is typed after substitution, as if the text had been written
    # in the file. A quoted value is a string, as the quotes say.
    if node.style is not None:
        return text
    tag = loader.resolve(yaml.ScalarNode, text, (True, False))
    if tag == "tag:yaml.org,2002:str":
        return text
    return loader.construct_object(
        yaml.ScalarNode(tag, text, node.start_mark, node.end_mark)
    )


def resolve(reference: str) -> Any:
    """Imports the longest module prefix of `reference` and returns the rest.

    `pkg.module.func` imports `pkg.module` and returns its `func`. `module.Class.attr`
    falls back to importing `module` and reading `Class.attr`.

    Raises:
        ValueError: If no prefix is a module, or the attribute is missing.
    """
    parts = reference.split(".")
    if len(parts) < 2 or not all(part.isidentifier() for part in parts):
        raise ValueError("expected a dotted path such as module.attribute")

    for end in range(len(parts) - 1, 0, -1):
        module_name = ".".join(parts[:end])
        try:
            target: Any = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            # Only a missing prefix means "try a shorter one". A module that exists
            # but fails on one of its own imports is reported as it is.
            if exc.name and (
                module_name == exc.name or module_name.startswith(exc.name + ".")
            ):
                continue
            raise ValueError(f"importing '{module_name}' failed: {exc}") from None
        except Exception as exc:
            raise ValueError(
                f"importing '{module_name}' failed: {type(exc).__name__}: {exc}"
            ) from None

        for attribute in parts[end:]:
            if not hasattr(target, attribute):
                raise ValueError(
                    f"'{module_name}' has no attribute '{'.'.join(parts[end:])}'"
                )
            target = getattr(target, attribute)
        return target

    raise ValueError(
        f"no module named '{parts[0]}'. Modules are found on sys.path, which "
        f"includes the folder of the YAML file"
    )


def _construct_python(loader: yaml.SafeLoader, node: yaml.Node) -> Any:
    line = node.start_mark.line + 1
    if not isinstance(node, yaml.ScalarNode):
        raise BlueprintError(
            f"{loader.name}:{line}: !python takes a dotted path such as "
            f"module.attribute"
        )
    reference = _substitute_at(loader, node, str(loader.construct_scalar(node)))
    try:
        return resolve(str(reference))
    except ValueError as exc:
        raise BlueprintError(f"{loader.name}:{line}: !python {reference}: {exc}")


BlueprintLoader.add_constructor("!python", _construct_python)
BlueprintLoader.add_constructor("tag:yaml.org,2002:str", _construct_str)


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

    The folder of the file is added to the front of `sys.path` before `!python`
    references are resolved, and stays there so later imports find the same modules.

    Raises:
        BlueprintError: If the file is not valid YAML, is not a mapping, or names a
            Python object that cannot be imported.
    """
    path = Path(path)
    # `!python` references resolve against the folder of the file first, so a
    # blueprint and the module beside it work from any working directory.
    folder = str(path.resolve().parent)
    if folder not in sys.path:
        sys.path.insert(0, folder)

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
