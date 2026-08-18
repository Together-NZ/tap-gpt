#!/usr/bin/env python3
"""Apply canonical dbt package revision pins across every client in this folder.

Each client keeps its own ``<client>/transform/packages.yml``. The pins drift:
some entries carry a stale tag, some carry none at all (which silently resolves
to ``main`` when the Dockerfile runs ``dbt deps``, so one bad upstream tag breaks
every unpinned client at once). This script rewrites those files in place using a
line-based edit, so comments, blank lines and the existing indentation survive.

Usage
-----
    # show what would change, touching nothing (default)
    python3 pin_dbt_package_revisions.py

    # apply the canonical map
    python3 pin_dbt_package_revisions.py --write

    # bump one package everywhere, then regenerate the lock files
    python3 pin_dbt_package_revisions.py --set tiktok=v2.0.7 --write --run-dbt-deps

    # CI guard: non-zero exit when any client is off the canonical map
    python3 pin_dbt_package_revisions.py --check
"""

from __future__ import annotations

import argparse
import difflib
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

# Canonical revision per package, keyed by repository name. hivestack and tiktok
# come from Contact; the rest are the revision the majority of clients already
# agree on.
CANONICAL_REVISIONS: dict[str, str] = {
    "Impact-CI-hivestack_models": "v2.0.1",
    "Impact-CI-tiktok_models": "v2.0.6",
    "Impact-CI-facebook_models": "v1.0.0",
    "Impact-CI-Linkedin_models": "v1.0.0",
    "Impact-CI-DV360_models": "v1.0.0",
    "Impact-CI-ttd_models": "v1.0.1",
    "Impact-CI-google_ads_models": "v1.0.0",
    "Impact-CI-ga4_models": "v1.0.0",
    "Impact-CI-cm360-models": "v1.0.0",
    "Impact-CI-dash_table_general_process": "v1.0.3",
    "Impact-CI-snapchat_models": "v1.0.0",
    "Impact-CI-reddit_models": "v1.0.0",
    "Impact-CI-Pinterest-models": "v1.0.0",
    "Impact-CI-Outbrain-dbt-package": "v1.0.0",
}

# Client-specific pins that deliberately differ from the canonical map. Only
# overridden when --force is passed.
CLIENT_OVERRIDES: dict[str, dict[str, str]] = {
    "contact": {"Impact-CI-google_ads_models": "v1.3.2"},
    "kiwibank": {"Impact-CI-google_ads_models": "v1.2.4"},
    "warehouse": {"Impact-CI-google_ads_models": "v1.1.3"},
    "realnz": {"Impact-CI-google_ads_models": "v2.0.1"},
}

# Packages intentionally left unpinned across all clients (they track main).
INTENTIONALLY_UNPINNED: set[str] = {"Impact-CI-testing_sum"}

GIT_ENTRY_RE = re.compile(r"^(?P<indent>\s*)-\s+git:\s*(?P<url>\S+)\s*(?P<trailing>#.*)?$")
REVISION_RE = re.compile(r"^(?P<indent>\s*)revision:\s*(?P<rev>\S+)(?P<trailing>\s*#.*)?\s*$")
NEXT_ENTRY_RE = re.compile(r"^\s*-\s|^\S")


def package_name(url: str) -> str:
    """Repository name for a git URL, e.g. ``Impact-CI-tiktok_models``."""
    return url.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")


def normalise(name: str) -> str:
    """Loose key so ``tiktok`` matches ``Impact-CI-tiktok_models`` on --set."""
    key = name.lower().removeprefix("impact-ci-")
    for suffix in ("_models", "-models", "-dbt-package"):
        key = key.removesuffix(suffix)
    return key


def build_revision_map(overrides: dict[str, str]) -> dict[str, str]:
    """Canonical map keyed by normalised name, with CLI overrides applied last."""
    revisions = {normalise(name): rev for name, rev in CANONICAL_REVISIONS.items()}
    revisions.update({normalise(name): rev for name, rev in overrides.items()})
    return revisions


@dataclass
class Change:
    line_no: int
    package: str
    before: str | None
    after: str

    def describe(self) -> str:
        action = "pin" if self.before is None else "bump"
        origin = "(unpinned)" if self.before is None else self.before
        return f"  {action:4} {self.package:38} {origin} -> {self.after}"


@dataclass
class ClientResult:
    client: str
    path: Path
    changes: list[Change]
    original: str
    updated: str
    skipped: list[str]

    @property
    def modified(self) -> bool:
        return self.original != self.updated


def find_revision_line(lines: list[str], entry_index: int) -> int | None:
    """Index of the ``revision:`` line belonging to the entry, if it has one."""
    for i in range(entry_index + 1, len(lines)):
        line = lines[i]
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if NEXT_ENTRY_RE.match(line):
            return None
        if REVISION_RE.match(line):
            return i
    return None


def process_file(
    path: Path,
    revisions: dict[str, str],
    client_overrides: dict[str, str],
    only_packages: set[str] | None,
    pin_missing_only: bool,
) -> ClientResult:
    client = path.parts[-3] if len(path.parts) >= 3 else path.stem
    original = path.read_text()
    lines = original.splitlines()
    changes: list[Change] = []
    skipped: list[str] = []
    insertions: list[tuple[int, str]] = []

    for index, line in enumerate(lines):
        entry = GIT_ENTRY_RE.match(line)
        if not entry:
            continue

        name = package_name(entry.group("url"))
        key = normalise(name)

        if name in INTENTIONALLY_UNPINNED:
            continue
        if only_packages is not None and key not in only_packages:
            continue

        target = client_overrides.get(key, revisions.get(key))
        if target is None:
            skipped.append(name)
            continue

        revision_index = find_revision_line(lines, index)

        if revision_index is None:
            # Indent the new key under the entry's mapping keys, not the dash.
            indent = " " * (len(entry.group("indent")) + 2)
            insertions.append((index + 1, f"{indent}revision: {target}"))
            changes.append(Change(index + 1, name, None, target))
            continue

        if pin_missing_only:
            continue

        current = REVISION_RE.match(lines[revision_index])
        assert current is not None
        if current.group("rev") == target:
            continue

        trailing = current.group("trailing") or ""
        lines[revision_index] = f"{current.group('indent')}revision: {target}{trailing}"
        changes.append(Change(revision_index + 1, name, current.group("rev"), target))

    for offset, (position, text) in enumerate(insertions):
        lines.insert(position + offset, text)

    updated = "\n".join(lines)
    # Preserve whether the original file ended with a newline.
    if original.endswith("\n"):
        updated += "\n"

    return ClientResult(client, path, changes, original, updated, skipped)


def run_dbt_deps(path: Path) -> bool:
    """Regenerate package-lock.yml so it does not go stale in CI."""
    transform_dir = path.parent
    print(f"  running dbt deps in {transform_dir}")
    result = subprocess.run(
        ["dbt", "deps"], cwd=transform_dir, capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  dbt deps failed ({result.returncode}):\n{result.stdout}{result.stderr}")
    return result.returncode == 0


def parse_set_arguments(values: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise SystemExit(f"--set expects package=revision, got {value!r}")
        name, revision = value.split("=", 1)
        if not name.strip() or not revision.strip():
            raise SystemExit(f"--set expects package=revision, got {value!r}")
        overrides[name.strip()] = revision.strip()
    return overrides


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply canonical dbt package revision pins across client repos.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Folder holding the client directories (default: this file's folder).",
    )
    parser.add_argument("--write", action="store_true", help="Apply the changes.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 if any client is off the map. Never writes.",
    )
    parser.add_argument(
        "--only",
        metavar="CLIENT",
        action="append",
        default=[],
        help="Limit to these clients. Repeatable.",
    )
    parser.add_argument(
        "--exclude",
        metavar="CLIENT",
        action="append",
        default=[],
        help="Skip these clients. Repeatable.",
    )
    parser.add_argument(
        "--set",
        metavar="PACKAGE=REVISION",
        action="append",
        default=[],
        dest="set_revisions",
        help="Override the canonical revision, e.g. --set tiktok=v2.0.7.",
    )
    parser.add_argument(
        "--package",
        metavar="PACKAGE",
        action="append",
        default=[],
        help="Only touch these packages. Repeatable. Defaults to all known ones.",
    )
    parser.add_argument(
        "--pin-missing-only",
        action="store_true",
        help="Add absent revisions but leave existing tags alone.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Also rewrite the client-specific pins in CLIENT_OVERRIDES.",
    )
    parser.add_argument(
        "--run-dbt-deps",
        action="store_true",
        help="Run dbt deps in every changed client to refresh package-lock.yml.",
    )
    parser.add_argument("--diff", action="store_true", help="Show a unified diff.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.write and args.check:
        raise SystemExit("--write and --check are mutually exclusive")

    revisions = build_revision_map(parse_set_arguments(args.set_revisions))
    explicit_packages = {normalise(name) for name in args.package} or None
    only_clients = {c.lower() for c in args.only}
    excluded_clients = {c.lower() for c in args.exclude}

    paths = sorted(args.root.glob("*/transform/packages.yml"))
    if not paths:
        raise SystemExit(f"no */transform/packages.yml found under {args.root}")

    results: list[ClientResult] = []
    for path in paths:
        client = path.parts[-3]
        if only_clients and client.lower() not in only_clients:
            continue
        if client.lower() in excluded_clients:
            continue

        client_overrides = (
            {}
            if args.force
            else {
                normalise(name): rev
                for name, rev in CLIENT_OVERRIDES.get(client, {}).items()
            }
        )
        results.append(
            process_file(
                path,
                revisions,
                client_overrides,
                explicit_packages,
                args.pin_missing_only,
            )
        )

    changed = [r for r in results if r.modified]
    unknown = sorted({name for r in results for name in r.skipped})

    for result in changed:
        print(f"{result.path}")
        for change in result.changes:
            print(change.describe())
        if args.diff:
            diff = difflib.unified_diff(
                result.original.splitlines(keepends=True),
                result.updated.splitlines(keepends=True),
                fromfile=f"a/{result.path}",
                tofile=f"b/{result.path}",
            )
            print("".join(diff))

    total_changes = sum(len(r.changes) for r in changed)
    print(
        f"\n{len(results)} client file(s) inspected, "
        f"{len(changed)} need changes, {total_changes} revision(s) affected."
    )

    if unknown:
        print(
            "\nNo canonical revision known for: "
            + ", ".join(unknown)
            + "\nAdd them to CANONICAL_REVISIONS or pass --set to pin them."
        )

    if args.check:
        return 1 if changed else 0

    if not args.write:
        if changed:
            print("\nDry run. Re-run with --write to apply.")
        return 0

    for result in changed:
        result.path.write_text(result.updated)
        print(f"wrote {result.path}")

    if args.run_dbt_deps:
        failures = [r.client for r in changed if not run_dbt_deps(r.path)]
        if failures:
            print("dbt deps failed for: " + ", ".join(failures))
            return 1
        if changed:
            print("Commit the regenerated transform/package-lock.yml files.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
