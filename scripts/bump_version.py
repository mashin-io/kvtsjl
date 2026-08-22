#!/usr/bin/env python3
"""Set package version in src/kvtsjl/__init__.py (hatch reads this for builds)."""

from __future__ import annotations

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
INIT = ROOT / "src/kvtsjl/__init__.py"
_VERSION_RE = re.compile(r"^__version__ = \"[^\"]*\"", re.MULTILINE)
_VERSION_FMT = re.compile(r"^\d+\.\d+\.\d+$")


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python scripts/bump_version.py X.Y.Z", file=sys.stderr)
        sys.exit(1)

    version = sys.argv[1]
    if not _VERSION_FMT.fullmatch(version):
        print(f"Invalid version {version!r} (expected X.Y.Z)", file=sys.stderr)
        sys.exit(1)

    text = INIT.read_text(encoding="utf-8")
    new_text, count = _VERSION_RE.subn(f'__version__ = "{version}"', text, count=1)
    if count != 1:
        print("Could not find __version__ in src/kvtsjl/__init__.py", file=sys.stderr)
        sys.exit(1)

    INIT.write_text(new_text, encoding="utf-8")
    print(f"Bumped to {version}")
    print()
    print("Next:")
    print(f"  git commit -am 'Release {version}'")
    print(f"  git tag {version}")
    print(f"  git push origin main && git push origin {version}")
    print(f"  gh release create {version} --title {version}")


if __name__ == "__main__":
    main()
