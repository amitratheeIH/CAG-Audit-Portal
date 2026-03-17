#!/usr/bin/env python3
"""
bump_schema_versions.py
-----------------------
Detects changed schema files (via git diff) and bumps their version fields.
Also updates the schema_versions map in any manifest.json files that reference
the changed schemas.

Version bump strategy:
  - Breaking change (removed required field, changed type): MAJOR
  - New required field, new enum value: MINOR
  - Description-only change, new optional field: PATCH
  - If unsure: MINOR (conservative)

In CI (--dry-run mode), only reports what would change without writing files.

Usage:
    python scripts/bump_schema_versions.py           # auto-bump changed schemas
    python scripts/bump_schema_versions.py --dry-run # report only, no writes
    python scripts/bump_schema_versions.py --schema atn.schema --level minor
"""

__version__ = "1.0"


import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = REPO_ROOT / "schemas"
REPORTS_DIR = REPO_ROOT / "reports"

VERSION_RE = re.compile(r'^(\d+)\.(\d+)$')


def parse_version(v: str) -> tuple[int, int]:
    m = VERSION_RE.match(v.strip())
    if not m:
        raise ValueError(f"Cannot parse version string: '{v}'")
    return int(m.group(1)), int(m.group(2))


def bump_version(v: str, level: str) -> str:
    major, minor = parse_version(v)
    if level == "major":
        return f"{major + 1}.0"
    elif level == "minor":
        return f"{major}.{minor + 1}"
    elif level == "patch":
        # We use MAJOR.MINOR only; patch just bumps minor
        return f"{major}.{minor + 1}"
    else:
        raise ValueError(f"Unknown bump level: {level}")


def get_changed_schemas() -> list[str]:
    """Return list of *.schema filenames that differ from HEAD~1."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "HEAD~1", "HEAD"],
            cwd=REPO_ROOT, capture_output=True, text=True, check=True
        )
        changed = result.stdout.splitlines()
    except subprocess.CalledProcessError:
        # Possibly first commit or shallow clone; diff against empty tree
        result = subprocess.run(
            ["git", "diff", "--name-only", "--cached"],
            cwd=REPO_ROOT, capture_output=True, text=True, check=True
        )
        changed = result.stdout.splitlines()

    return [
        Path(f).name
        for f in changed
        if f.startswith("schemas/") and f.endswith(".schema")
    ]


def infer_bump_level(schema_path: Path) -> str:
    """
    Heuristic: compare old schema (HEAD~1) vs current to guess bump level.
    Falls back to 'minor' if git history unavailable.
    """
    try:
        old = subprocess.run(
            ["git", "show", f"HEAD~1:schemas/{schema_path.name}"],
            cwd=REPO_ROOT, capture_output=True, text=True, check=True
        ).stdout
        old_schema = json.loads(old)
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        return "minor"

    new_schema = json.loads(schema_path.read_text())

    old_required = set(old_schema.get("required", []))
    new_required = set(new_schema.get("required", []))
    old_props = set(old_schema.get("properties", {}).keys())
    new_props = set(new_schema.get("properties", {}).keys())

    removed_required = old_required - new_required
    added_required = new_required - old_required
    removed_props = old_props - new_props

    if removed_required or removed_props:
        return "major"
    if added_required:
        return "minor"
    return "minor"   # safe default


def bump_schema_file(schema_path: Path, level: str, dry_run: bool) -> tuple[str, str] | None:
    """Bump version in schema file. Returns (old_version, new_version) or None.

    All CAG schemas use '$version' (not 'version') as the version field.
    """
    content = schema_path.read_text()
    schema = json.loads(content)

    # Schemas use '$version'; fall back to 'version' for any legacy files
    version_key = "$version" if "$version" in schema else "version"
    old_version = schema.get(version_key)
    if not old_version:
        print(f"  WARN  {schema_path.name}: no '$version' or 'version' field found, skipping")
        return None

    new_version = bump_version(old_version, level)
    schema[version_key] = new_version

    if not dry_run:
        schema_path.write_text(json.dumps(schema, indent=2, ensure_ascii=False) + "\n")

    return old_version, new_version


def update_manifests(schema_name: str, new_version: str, dry_run: bool):
    """Update schema_versions in all manifest.json files referencing this schema."""
    base_name = schema_name.replace(".schema", "")
    updated = []

    for manifest_path in REPORTS_DIR.rglob("manifest.json"):
        manifest = json.loads(manifest_path.read_text())
        schema_versions = manifest.get("schema_versions", {})

        if base_name in schema_versions:
            schema_versions[base_name] = new_version
            manifest["schema_versions"] = schema_versions
            if not dry_run:
                manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
            updated.append(str(manifest_path.relative_to(REPO_ROOT)))

    return updated


def main():
    parser = argparse.ArgumentParser(description="Bump schema version numbers after changes")
    parser.add_argument("--dry-run", action="store_true", help="Report without writing")
    parser.add_argument("--schema", help="Specific schema filename (e.g. atn.schema)")
    parser.add_argument("--level", choices=["major", "minor", "patch"], help="Force bump level")
    args = parser.parse_args()

    if args.schema:
        changed_schemas = [args.schema]
    else:
        changed_schemas = get_changed_schemas()

    if not changed_schemas:
        print("No schema changes detected.")
        sys.exit(0)

    print(f"{'DRY RUN — ' if args.dry_run else ''}Processing {len(changed_schemas)} changed schema(s):\n")

    for schema_name in changed_schemas:
        schema_path = SCHEMAS_DIR / schema_name
        if not schema_path.exists():
            print(f"  SKIP  {schema_name} (file not found)")
            continue

        level = args.level or infer_bump_level(schema_path)
        result = bump_schema_file(schema_path, level, args.dry_run)

        if result:
            old_v, new_v = result
            updated = update_manifests(schema_name, new_v, args.dry_run)
            action = "would bump" if args.dry_run else "bumped"
            print(f"  {schema_name}: {old_v} → {new_v}  [{level}]  {action}")
            for m in updated:
                print(f"    ↳ updated {m}")

    if args.dry_run:
        print("\nDry run complete. No files modified.")


if __name__ == "__main__":
    main()
