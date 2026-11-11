"""Check that every ``AGENTS.md`` chain fits Codex's 32 KiB.

Codex reads the ``AGENTS.md`` files from the repository root down to its working
directory together, up to ``project_doc_max_bytes`` (32,768 bytes by default), and
cuts the rest without telling the model. A chain is the root file plus every nested
one on the way to a nested file, so the deepest chains are the ones that overflow.
In each directory Codex reads ``AGENTS.override.md`` instead of ``AGENTS.md`` when
both exist. A symlinked one is refused: Codex reads its target, which a commit can
grow without touching the link, so the hook would not run.
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
# Codex reads the first of these that exists in a directory.
NAMES = ("AGENTS.override.md", NAME)
SYMLINK = "120000"

SHRINK = """\
Shrink the chain before adding to it, section by section, as CODING_GUIDELINES.md
§ Agent rule files says: delete what the code already says; cut a rule a test
enforces to one line naming the guard; delete why and what was measured, keeping the
ADR number; move a rule one file must keep into its docstring; move a rule for one
directory into that directory's AGENTS.md. List what you deleted in the PR."""


def _git(*args: str, stdin: str | None = None) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        input=stdin,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def _staged_sizes() -> dict[str, int]:
    blobs = {}
    for entry in filter(None, _git("ls-files", "-s", "-z").split("\0")):
        meta, name = entry.split("\t", 1)
        if PurePosixPath(name).name not in NAMES:
            continue
        mode, sha = meta.split()[:2]
        if mode == SYMLINK:
            raise SystemExit(f"error: {name} is a symlink; commit the file itself")
        blobs[name] = sha
    if not blobs:
        return {}
    # Send each path's position, not the path itself:
    # cat-file drops leading whitespace from %(rest).
    names = list(blobs)
    request = "".join(f"{blobs[name]} {i}\n" for i, name in enumerate(names))
    answer = _git("cat-file", "--batch-check=%(objectsize) %(rest)", stdin=request)
    pairs = (line.split() for line in answer.splitlines())
    return {names[int(i)]: int(size) for size, i in pairs}


def chains() -> list[tuple[int, list[tuple[str, int]]]]:
    """Each chain's total and its files with their sizes, largest total first."""
    size = _staged_sizes()
    result = []
    for directory in sorted({PurePosixPath(name).parent for name in size}, key=str):
        members = []
        for parent in reversed((directory / NAME).parents):
            present = [str(parent / n) for n in NAMES if str(parent / n) in size]
            if present:
                members.append((present[0], size[present[0]]))
        result.append((sum(n for _, n in members), members))
    return sorted(result, key=lambda c: c[0], reverse=True)


def main() -> int:
    found = chains()
    if not found:
        print(f"no {NAME} tracked")
        return 0
    over = [c for c in found if c[0] > BUDGET]
    for total, members in over:
        last, excess = members[-1][0], total - BUDGET
        print(f"error: {last} chain is {total:,} bytes, {excess:,} over {BUDGET:,}")
        for link, n in members:
            print(f"  {n:>7,}  {link}")
    if over:
        print(SHRINK)
        return 1
    total, members = found[0]
    last = members[-1][0]
    print(f"{NAME} chains fit: the largest is {total:,} of {BUDGET:,} bytes ({last})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
