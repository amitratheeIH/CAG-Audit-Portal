#!/usr/bin/env python3
"""
validate_report.py
------------------
Validates one or all CAG audit report packages against JSON schemas.

Usage:
    python scripts/validate_report.py --all
    python scripts/validate_report.py --product-id AR06-CAG-2023-STATE-MP
    python scripts/validate_report.py --path reports/audit_report/2023/in-mp/AR06-CAG-2023-STATE-MP
"""

__version__ = "1.0"


import argparse
import json
import sys
from pathlib import Path

import jsonschema
from jsonschema import Draft7Validator, RefResolver

sys.path.insert(0, str(Path(__file__).resolve().parent))
import repo_layout as rl

_schema_cache: dict[str, dict] = {}


def load_schema(schema_file: str) -> dict:
    if schema_file not in _schema_cache:
        path = rl.SCHEMAS_DIR / schema_file
        if not path.exists():
            raise FileNotFoundError(f"Schema not found: {path}")
        _schema_cache[schema_file] = json.loads(path.read_text())
    return _schema_cache[schema_file]


def make_resolver() -> RefResolver:
    schema_store = {}
    for schema_path in rl.SCHEMAS_DIR.glob("*.schema"):
        schema = json.loads(schema_path.read_text())
        # Store by file URI so $ref resolution works even without $id fields
        file_uri = schema_path.as_uri()
        schema_store[file_uri] = schema
        # Also store by $id if present
        if "$id" in schema:
            schema_store[schema["$id"]] = schema
    base_uri = rl.SCHEMAS_DIR.as_uri() + "/"
    return RefResolver(base_uri=base_uri, referrer={}, store=schema_store)


def validate_json(data, schema: dict, resolver: RefResolver, source: Path) -> list[str]:
    # Create a schema-scoped resolver so internal $refs like #/definitions/...
    # resolve against this schema's own store, not an empty referrer.
    schema_resolver = RefResolver(
        base_uri=resolver.resolution_scope,
        referrer=schema,
        store=resolver.store,
    )
    validator = Draft7Validator(schema, resolver=schema_resolver)
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))
    return [
        f"  [{source.name}] {'.'.join(str(p) for p in e.path) or '(root)'}: {e.message}"
        for e in errors
    ]


def validate_ndjson(path: Path, schema: dict, resolver: RefResolver) -> list[str]:
    errors = []
    validator = Draft7Validator(schema, resolver=resolver)
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"  [{path.name}:{lineno}] JSON parse error: {exc}")
            continue
        for e in sorted(validator.iter_errors(obj), key=lambda e: list(e.path)):
            field = '.'.join(str(p) for p in e.path) or '(root)'
            errors.append(f"  [{path.name}:{lineno}] {field}: {e.message}")
    return errors


def validate_report_dir(report_dir: Path, resolver: RefResolver) -> dict[str, list[str]]:
    """
    Validate a single report directory against the canonical layout:

      manifest.json                   ← required
      metadata.json                   ← required
      structure.json                  ← required
      units/          *.json          ← required; one per content_unit
      blocks/         content_block_*.ndjson  ← required; one per chapter
      atn/            atn_*.json      ← optional
      datasets/       *.json          ← optional
        ndjson/       *.ndjson        ← optional; large dataset companions
      footnotes/      footnotes_*.json ← optional
      pdfs/                           ← not schema-validated (presence only)
      assets/                         ← not schema-validated
      embeddings/                     ← pipeline-generated; not validated here
    """
    all_errors: dict[str, list[str]] = {}

    def record(file_path: Path, errs: list[str]):
        if errs:
            all_errors[str(file_path.relative_to(report_dir))] = errs

    def missing(label: str):
        all_errors[label] = [f"  [{label}] Missing"]

    # ── manifest (gate: abort if missing) ─────────────────────────────────────
    manifest_path = report_dir / "manifest.json"
    if not manifest_path.exists():
        missing("manifest.json")
        return all_errors

    manifest = json.loads(manifest_path.read_text())
    record(manifest_path, validate_json(
        manifest, load_schema("manifest.schema"), resolver, manifest_path
    ))

    # ── metadata.json — validate common and specific separately ─────────────
    # metadata.json = product.metadata = { "common": {...}, "specific": {...} }
    # common  → always validated against common_metadata.schema
    # specific → validated against the product-type-specific schema
    # Add new product types here as their specific schemas are created.
    SPECIFIC_SCHEMA_MAP = {
        "audit_report": "audit_report_metadata.schema",
    }
    product_type = manifest.get("product_type", "")
    specific_schema_file = SPECIFIC_SCHEMA_MAP.get(product_type)

    metadata_path = report_dir / "metadata.json"
    if not metadata_path.exists():
        missing("metadata.json")
    else:
        metadata_doc = json.loads(metadata_path.read_text())

        # Validate common section
        common_section = metadata_doc.get("common")
        if common_section is None:
            all_errors.setdefault("metadata.json", []).append(
                "  [metadata.json] missing required key 'common'"
            )
        else:
            errs = validate_json(
                common_section, load_schema("common_metadata.schema"), resolver, metadata_path
            )
            record(metadata_path, errs)

        # Validate specific section
        specific_section = metadata_doc.get("specific")
        if specific_section is None:
            all_errors.setdefault("metadata.json", []).append(
                "  [metadata.json] missing required key 'specific'"
            )
        elif specific_schema_file is None:
            pass  # No product-specific schema defined yet — skip silently
        else:
            errs = validate_json(
                specific_section, load_schema(specific_schema_file), resolver, metadata_path
            )
            record(metadata_path, errs)

    # ── structure.json ────────────────────────────────────────────────────────
    structure_path = report_dir / "structure.json"
    if not structure_path.exists():
        missing("structure.json")
    else:
        record(structure_path, validate_json(
            json.loads(structure_path.read_text()), load_schema("structure.schema"), resolver, structure_path
        ))

    # ── units/ ── *.json ──────────────────────────────────────────────────────
    units_d = rl.units_dir(report_dir)
    if not units_d.exists():
        missing("units/")
    else:
        unit_files = sorted(units_d.glob("*.json"))
        if not unit_files:
            missing("units/ (no .json files)")
        for uf in unit_files:
            try:
                json.loads(uf.read_text())
            except json.JSONDecodeError as exc:
                record(uf, [f"  [{uf.name}] JSON parse error: {exc}"])

    # ── blocks/ ── content_block_*.ndjson ────────────────────────────────────
    blocks_d = rl.blocks_dir(report_dir)
    cb_schema = load_schema("content_block.schema")
    if not blocks_d.exists():
        missing("blocks/")
    else:
        ndjson_files = rl.block_ndjson_files(report_dir)
        if not ndjson_files:
            missing("blocks/ (no content_block_*.ndjson files)")
        for nf in ndjson_files:
            record(nf, validate_ndjson(nf, cb_schema, resolver))

    # ── atn/ ── atn_*.json (optional) ─────────────────────────────────────────
    atn_d = rl.atn_dir(report_dir)
    atn_schema = load_schema("atn.schema")
    if atn_d.exists():
        for af in rl.atn_json_files(report_dir):
            record(af, validate_json(
                json.loads(af.read_text()), atn_schema, resolver, af
            ))

    # ── datasets/ ── *.json (optional) ───────────────────────────────────────
    datasets_d = rl.datasets_dir(report_dir)
    ds_schema = load_schema("dataset.schema")
    if datasets_d.exists():
        for df in rl.dataset_json_files(report_dir):
            record(df, validate_json(
                json.loads(df.read_text()), ds_schema, resolver, df
            ))

    # ── footnotes/ ── footnotes_*.json (optional) ─────────────────────────────
    footnotes_d = rl.footnotes_dir(report_dir)
    fn_schema = load_schema("footnote.schema")
    if footnotes_d.exists():
        for ff in rl.footnote_json_files(report_dir):
            record(ff, validate_json(
                json.loads(ff.read_text()), fn_schema, resolver, ff
            ))

    return all_errors


def resolve_dirs(args) -> list[Path]:
    if args.all:
        dirs = rl.all_report_dirs()
        if not dirs:
            print("No report directories found under reports/")
            sys.exit(0)
        return dirs
    if args.product_id:
        found = rl.locate_report(args.product_id)
        if not found:
            print(f"ERROR: product_id '{args.product_id}' not found anywhere under reports/")
            sys.exit(1)
        return [found]
    return [Path(args.path).resolve()]


def main():
    parser = argparse.ArgumentParser(description="Validate CAG audit report packages")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--product-id", help="product_id — searches the full reports/ tree")
    group.add_argument("--path", help="Explicit path to report folder")
    group.add_argument("--all", action="store_true", help="Validate all reports")
    args = parser.parse_args()

    resolver = make_resolver()
    dirs = resolve_dirs(args)
    failed = 0

    for report_dir in dirs:
        if not report_dir.is_dir():
            print(f"SKIP  {report_dir}  (not a directory)")
            continue

        errors = validate_report_dir(report_dir, resolver)
        try:
            label = str(report_dir.relative_to(rl.REPO_ROOT))
        except ValueError:
            label = str(report_dir)

        if errors:
            failed += 1
            print(f"\nFAIL  {label}")
            for filename, errs in errors.items():
                print(f"  ── {filename}")
                for e in errs:
                    print(e)
        else:
            print(f"OK    {label}")

    print(f"\n{'─'*60}")
    total = len(dirs)
    print(f"Results: {total - failed}/{total} passed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
