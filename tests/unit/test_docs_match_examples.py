"""Every example script shown in the documentation must match the file it came from.

These pages used to hold hand-copied source, and all six had drifted: one showed a
`TaskEvent` returned directly where the script returns `.model_dump(mode="json")`,
another documented a capacity schedule the script does not have, and two carried an S3
path that had been corrected in the script months earlier.

Snippet includes fixed that by never copying, but the raw markdown then showed a
`--8<--` marker rather than code to anyone reading the file on GitHub. So the source is
copied in again, and this test is what stops it drifting a second time.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BLOCK = re.compile(r'```python title="(examples/[^"]+)"\n(.*?)\n```', re.S)


def _blocks():
    for page in sorted((ROOT / "docs").rglob("*.md")):
        for path, code in BLOCK.findall(page.read_text(encoding="utf-8")):
            yield page.relative_to(ROOT), path, code


def test_at_least_one_page_shows_a_script():
    """A regex that silently matches nothing would make every other case vacuous."""
    assert len(list(_blocks())) >= 10


@pytest.mark.parametrize(
    "page,path,code",
    list(_blocks()),
    ids=[f"{page}:{path}" for page, path, _ in _blocks()],
)
def test_documented_source_matches_the_script(page, path, code):
    script = ROOT / path
    assert script.is_file(), f"{page} shows {path}, which does not exist"
    assert code == script.read_text(encoding="utf-8").rstrip("\n"), (
        f"{page} has drifted from {path}. Copy the script in again rather than "
        f"editing the page."
    )
