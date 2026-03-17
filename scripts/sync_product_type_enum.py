#!/usr/bin/env python3
"""
sync_product_type_enum.py
-------------------------
Syncs the product_type enum values from taxonomy_product_types.json into
every schema file that contains a product_type enum.

Affected schemas (auto-detected by scanning for "product_type" enum):
  - common_metadata.schema
  - catalog.schema
  - manifest.schema
  - product.schema

Usage:
    python scripts/sync_product_type_enum.py           # apply changes
    python scripts/sync_product_type_enum.py --check   # CI check mode (no writes, exit 1 if out of sync)
    python scripts/sync_product_type_enum.py --dry-run # show what would change
"""

__version__ = "1.0"


import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import repo_layout as rl


def load_product_type_ids() -> list[str]:
    path = rl.TAXONOMIES_DIR / "taxonomy_product_types.json"
    data = json.loads(path.read_text())
    return [e["id"] for e in data["entries"]]


def find_product_type_enum_paths(schema: dict, path: list = None) -> list[list]:
    """
    Recursively find all JSON paths in the schema that are an enum for product_type.
    Returns list of key-path lists.
    """
    if path is None:
        path = []
    results = []
    if isinstance(schema, dict):
        # A property named product_type with an enum sibling, OR
        # a field that is already an enum adjacent to a key "product_type" somewhere in path
        if "enum" in schema and "type" in schema and schema.get("type") == "string":
            # Check if parent context suggests this is the product_type field
            if path and path[-1] == "product_type":
                results.append(path[:])
        for k, v in schema.items():
            results.extend(find_product_type_enum_paths(v, path + [k]))
    elif isinstance(schema, list):
        for i, v in enumerate(schema):
            results.extend(find_product_type_enum_paths(v, path + [i]))
    return results


def get_at_path(obj: dict, path: list):
    for key in path:
        obj = obj[key]
    return obj


def set_at_path(obj: dict, path: list, value):
    for key in path[:-1]:
        obj = obj[key]
    obj[path[-1]] = value


def sync_schema(schema_path: Path, new_enum: list[str], dry_run: bool, check: bool) -> bool:
    """
    Find and update all product_type enums in the schema.
    Returns True if any changes were made (or would be made).
    """
    schema = json.loads(schema_path.read_text())
    paths = find_product_type_enum_paths(schema)

    if not paths:
        return False

    changed = False
    for path in paths:
        current_enum = get_at_path(schema, path + ["enum"])
        if current_enum != new_enum:
            changed = True
            if not check and not dry_run:
                set_at_path(schema, path + ["enum"], new_enum)

    if changed:
        if dry_run or check:
            print(f"  OUT OF SYNC  {schema_path.name}")
        else:
            # Write back with consistent formatting
            schema_path.write_text(json.dumps(schema, ensure_ascii=False, indent=2) + "\n")
            print(f"  UPDATED      {schema_path.name}")
    else:
        print(f"  OK           {schema_path.name}")

    return changed


def main():
    parser = argparse.ArgumentParser(
        description="Sync product_type enum from taxonomy into schemas"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="CI mode: check for drift without writing; exit 1 if out of sync",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing",
    )
    args = parser.parse_args()

    new_enum = load_product_type_ids()
    print(f"product_type values from taxonomy ({len(new_enum)}): {new_enum}")
    print()

    any_changed = False
    for schema_path in sorted(rl.SCHEMAS_DIR.glob("*.schema")):
        schema = json.loads(schema_path.read_text())
        # Quick pre-check: only process schemas that mention product_type
        raw = schema_path.read_text()
        if "product_type" not in raw:
            continue
        changed = sync_schema(schema_path, new_enum, dry_run=args.dry_run, check=args.check)
        any_changed = any_changed or changed

    print()
    if any_changed:
        if args.check:
            print("FAIL — product_type enum is out of sync with taxonomy. Run sync_product_type_enum.py to fix.")
            sys.exit(1)
        elif args.dry_run:
            print("Dry run — no files written. Remove --dry-run to apply.")
        else:
            print("Sync complete.")
    else:
        print("All schemas are in sync with taxonomy.")


if __name__ == "__main__":
    main()
