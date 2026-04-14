"""Auto-generate reference pages for adapters and normalizers at build time.

Run automatically by mkdocs-gen-files during ``mkdocs build`` / ``mkdocs serve``.
Any new submodule added to ``nibble.adapters`` or ``nibble.normalizer`` is picked
up without any manual edits to the docs.
"""

from __future__ import annotations

import importlib
import pkgutil

import mkdocs_gen_files

_PAGES = [
    (
        "nibble.adapters",
        "reference/adapters.md",
        "Adapters",
        "Feed adapter plugins that fetch vehicle data and return a `FeedMessage`.",
        True,  # include package __init__ as a "Factory" section
    ),
    (
        "nibble.normalizer",
        "reference/normalizers.md",
        "Normalizers",
        "Agency-specific feed normalizers that fix quirks before parsing.",
        False,
    ),
]

# Human-readable section headings for known submodule names.
_DISPLAY_NAMES: dict[str, str] = {
    "base": "Base class",
    "default": "Default (pass-through)",
    "gtfs_rt": "GTFS-RT",
    "passio": "Passio GO!",
    "ripta": "RIPTA",
    "mwrta": "MWRTA",
    "brta": "BRTA",
    "vta": "VTA",
    "trillium": "Trillium",
    "swiv": "Swiv",
    "routematch": "RouteMatch",
}

# Submodules that should appear before the rest (in this order).
_FIRST = ("base", "default")


def _display(name: str) -> str:
    return _DISPLAY_NAMES.get(name, name.replace("_", " ").title())


for pkg_name, out_path, title, description, include_init in _PAGES:
    pkg = importlib.import_module(pkg_name)
    submodules = [name for _, name, _ in pkgutil.iter_modules(pkg.__path__)]

    # base/default first, everything else alphabetically.
    ordered = [n for n in _FIRST if n in submodules]
    ordered += sorted(n for n in submodules if n not in _FIRST)

    lines: list[str] = [f"# {title}\n\n{description}\n"]

    if include_init:
        lines += ["\n## Factory\n\n", f"::: {pkg_name}\n"]

    for name in ordered:
        lines += [f"\n## {_display(name)}\n\n", f"::: {pkg_name}.{name}\n"]

    with mkdocs_gen_files.open(out_path, "w") as f:
        f.writelines(lines)
