"""Check that every ``AGENTS.md`` chain fits Codex's 32 KiB.

Codex reads the ``AGENTS.md`` files from the repository root down to its working
directory together, up to ``project_doc_max_bytes`` (32,768 bytes by default), and
cuts the rest without telling the model. A chain is the root file plus every nested
one on the way to a nested file, so the deepest chains are the ones that overflow.
Sizes are those of the staged blobs, so what is checked is what gets committed, a
file a sparse checkout leaves out included, and a copy under a virtualenv or
``node_modules`` does not count.

Usage::

    python3 scripts/check_agents_md.py
"""

from __future__ import annotations

import subprocess
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parent.parent
BUDGET = 32_768
NAME = "AGENTS.md"

SHRINK = """\
Shrink the chain before adding to it, section by section, as CODING_GUIDELINES.md
§ Agent rule files says: delete what the code already says; cut a rule a test
enforces to one line naming the guard; delete why and what was measured, keeping the
ADR number; move a rule one file must keep into its docstring; move a rule for one
directory into that directory's AGENTS.md. List what you deleted in the PR."""


def _git(*args: str, stdin: str | None = None) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, input=stdin, capture_output=True, text=True, check=True
    ).stdout


def _staged_sizes() -> dict[str, int]:
    blobs = {}
    for entry in filter(None, _git("ls-files", "-s", "-z").split("\0")):
        meta, name = entry.split("\t", 1)
        if PurePosixPath(name).name == NAME:
            blobs[name] = meta.split()[1]
    if not blobs:
        return {}
    # Send each path's position, not the path itself:
    # cat-file drops leading whitespace from %(rest).
    names = list(blobs)
    request = "".join(f"{blobs[name]} {i}\n" for i, name in enumerate(names))
    answer = _git("cat-file", "--batch-check=%(objectsize) %(rest)", stdin=request)
    return {names[int(i)]: int(size) for size, i in (line.split() for line in answer.splitlines())}


def chains() -> list[tuple[int, list[tuple[str, int]]]]:
    """Each chain's total and its files with their sizes, largest total first."""
    size = _staged_sizes()
    result = []
    for name in sorted(size):
        links = (str(parent / NAME) for parent in reversed(PurePosixPath(name).parents))
        members = [(link, size[link]) for link in links if link in size]
        result.append((sum(n for _, n in members), members))
    return sorted(result, key=lambda c: c[0], reverse=True)


def main() -> int:
    found = chains()
    if not found:
        print(f"no {NAME} tracked")
        return 0
    over = [c for c in found if c[0] > BUDGET]
    for total, members in over:
        print(f"error: {members[-1][0]} chain is {total:,} bytes, {total - BUDGET:,} over {BUDGET:,}")
        for link, n in members:
            print(f"  {n:>7,}  {link}")
    if over:
        print(SHRINK)
        return 1
    total, members = found[0]
    print(f"{NAME} chains fit: the largest is {total:,} of {BUDGET:,} bytes ({members[-1][0]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
