#!/usr/bin/env python3
"""
validate_content_blocks.py
--------------------------
Validates content block NDJSON files against the content_block schema.

Checks performed:
  1. JSON parse error on any line
  2. Missing top-level required fields (block_id, block_type, content)
  3. Unknown top-level fields (additionalProperties: false at block level)
  4. block_type not in enum
  5. Missing required content fields (per block_type allOf rule)
  6. Unknown content fields (additionalProperties: false per block_type)
  7. Duplicate block_id within a file
  8. $version field read correctly from schema (uses '$version' not 'version')

Usage:
    # Validate a single NDJSON file against the schema:
    python validate_content_blocks.py \\
        --schema schemas/content_block.schema \\
        --ndjson blocks/content_block_CH01.ndjson

    # Validate all content_block_*.ndjson files in a report directory:
    python validate_content_blocks.py \\
        --schema schemas/content_block.schema \\
        --report-dir reports/audit_report/2023/state/in-mp/AR06-CAG-2023-STATE-MP

    # Validate and show a summary of block types found:
    python validate_content_blocks.py \\
        --schema schemas/content_block.schema \\
        --ndjson blocks/content_block_CH01.ndjson \\
        --summary

Exit codes:
    0  all blocks valid
    1  one or more validation errors found
    2  usage/file error
"""

__version__ = "1.0"


import argparse
import json
import sys
from collections import Counter
from pathlib import Path


# ---------------------------------------------------------------------------
# Schema parsing
# ---------------------------------------------------------------------------

def load_schema(schema_path: Path) -> dict:
    try:
        return json.loads(schema_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"ERROR: Schema file not found: {schema_path}", file=sys.stderr)
        sys.exit(2)
    except json.JSONDecodeError as exc:
        print(f"ERROR: Schema is not valid JSON: {exc}", file=sys.stderr)
        sys.exit(2)


def build_rules(schema: dict) -> dict[str, dict]:
    """
    Parse the allOf if/then rules from the schema.
    Returns {block_type: {required, allowed, addl_props_allowed}}.
    """
    rules: dict[str, dict] = {}
    for rule in schema.get("allOf", []):
        bt = (
            rule
            .get("if", {})
            .get("properties", {})
            .get("block_type", {})
            .get("const")
        )
        if not bt:
            continue
        then_content = rule.get("then", {}).get("properties", {}).get("content", {})
        rules[bt] = {
            "required": then_content.get("required", []),
            "allowed":  set(then_content.get("properties", {}).keys()),
            "strict":   then_content.get("additionalProperties") is False,
        }
    return rules


def get_top_level_info(schema: dict) -> tuple[set[str], set[str], list[str]]:
    """
    Returns (top_allowed_fields, block_type_enum, top_required_fields).
    """
    props       = schema.get("properties", {})
    top_allowed = set(props.keys())
    bt_enum     = set(props.get("block_type", {}).get("enum", []))
    top_required = schema.get("required", [])
    return top_allowed, bt_enum, top_required


def get_schema_version(schema: dict) -> str:
    # All CAG schemas use '$version', not 'version'
    return schema.get("$version") or schema.get("version") or "unknown"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_block(
    block: dict,
    lineno: int,
    top_allowed: set[str],
    top_required: list[str],
    bt_enum: set[str],
    rules: dict[str, dict],
) -> list[str]:
    errors: list[str] = []
    bid = block.get("block_id", f"<line {lineno}>")

    # 1. Top-level required fields
    for req in top_required:
        if req not in block:
            errors.append(f"  L{lineno} [{bid}] missing required field: '{req}'")

    # 2. Unknown top-level fields
    for k in block:
        if k not in top_allowed:
            errors.append(f"  L{lineno} [{bid}] unknown top-level field: '{k}'")

    # 3. block_type in enum
    bt = block.get("block_type", "")
    if not bt:
        errors.append(f"  L{lineno} [{bid}] missing block_type")
        return errors
    if bt not in bt_enum:
        errors.append(f"  L{lineno} [{bid}] unknown block_type: '{bt}' (not in schema enum)")
        return errors

    # 4. Content field checks
    content = block.get("content")
    if content is None:
        errors.append(f"  L{lineno} [{bid}] missing 'content'")
        return errors
    if not isinstance(content, dict):
        errors.append(f"  L{lineno} [{bid}] 'content' must be an object, got {type(content).__name__}")
        return errors

    rule = rules.get(bt)
    if rule is None:
        # block_type is in enum but has no allOf rule — no content constraints
        return errors

    # 5. Missing required content fields
    for req in rule["required"]:
        if req not in content:
            errors.append(
                f"  L{lineno} [{bid}] [{bt}] missing required content field: '{req}'"
            )

    # 6. Unknown content fields (when additionalProperties: false)
    if rule["strict"]:
        for k in content:
            if k not in rule["allowed"]:
                errors.append(
                    f"  L{lineno} [{bid}] [{bt}] invalid content field: '{k}'"
                    f" (allowed: {sorted(rule['allowed'])})"
                )

    return errors


def validate_ndjson(
    ndjson_path: Path,
    top_allowed: set[str],
    top_required: list[str],
    bt_enum: set[str],
    rules: dict[str, dict],
) -> tuple[list[str], Counter]:
    """
    Validate every line of an NDJSON file.
    Returns (all_errors, block_type_counter).
    """
    all_errors: list[str] = []
    seen_ids: dict[str, int] = {}       # block_id → first lineno
    bt_counter: Counter = Counter()

    try:
        raw = ndjson_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return [f"  File not found: {ndjson_path}"], Counter()
    except OSError as exc:
        return [f"  Cannot read file: {exc}"], Counter()

    for lineno, line in enumerate(raw.splitlines(), 1):
        line = line.strip()
        if not line:
            continue

        # JSON parse check
        try:
            block = json.loads(line)
        except json.JSONDecodeError as exc:
            all_errors.append(f"  L{lineno} JSON parse error: {exc}")
            continue

        # Duplicate block_id check
        bid = block.get("block_id")
        if bid:
            if bid in seen_ids:
                all_errors.append(
                    f"  L{lineno} [{bid}] duplicate block_id"
                    f" (first seen at L{seen_ids[bid]})"
                )
            else:
                seen_ids[bid] = lineno

        bt_counter[block.get("block_type", "<missing>")] += 1

        # Field validation
        block_errors = validate_block(
            block, lineno, top_allowed, top_required, bt_enum, rules
        )
        all_errors.extend(block_errors)

    return all_errors, bt_counter


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(bt_counter: Counter, total_blocks: int):
    print(f"\n  Block type summary ({total_blocks} blocks):")
    for bt, count in bt_counter.most_common():
        print(f"    {bt:<30} {count}")


def print_result(
    ndjson_path: Path,
    errors: list[str],
    bt_counter: Counter,
    show_summary: bool,
):
    total = sum(bt_counter.values())
    if errors:
        print(f"\nFAIL  {ndjson_path}  ({total} blocks, {len(errors)} error(s))")
        for e in errors:
            print(e)
    else:
        print(f"OK    {ndjson_path}  ({total} blocks)")

    if show_summary:
        print_summary(bt_counter, total)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def resolve_ndjson_files(args) -> list[Path]:
    """Resolve NDJSON files for --ndjson and --report-dir modes."""
    if args.ndjson:
        p = Path(args.ndjson)
        if not p.exists():
            print(f"ERROR: NDJSON file not found: {p}", file=sys.stderr)
            sys.exit(2)
        return [p]

    report_dir = Path(args.report_dir)
    blocks_dir = report_dir / "blocks"
    if not blocks_dir.exists():
        print(f"ERROR: blocks/ directory not found in {report_dir}", file=sys.stderr)
        sys.exit(2)
    files = sorted(blocks_dir.glob("content_block_*.ndjson"))
    if not files:
        print(f"ERROR: No content_block_*.ndjson files found in {blocks_dir}", file=sys.stderr)
        sys.exit(2)
    return files


def resolve_schema_path(args) -> Path:
    """
    Resolve the content_block.schema path.
    --schema takes explicit precedence.
    In --all mode without --schema, auto-detect from repo_layout.SCHEMAS_DIR.
    """
    if args.schema:
        return Path(args.schema)
    # --all without --schema: auto-detect via repo_layout
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import repo_layout as rl
        return rl.SCHEMAS_DIR / "content_block.schema"
    except ImportError:
        print("ERROR: --schema is required when repo_layout.py is not available", file=sys.stderr)
        sys.exit(2)


def main():
    parser = argparse.ArgumentParser(
        description="Validate content block NDJSON files against content_block.schema",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--schema",
        required=False,
        default=None,
        help="Path to content_block.schema. Auto-detected from repo_layout when using --all.",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--ndjson",
        help="Path to a single content_block_*.ndjson file",
    )
    source.add_argument(
        "--report-dir",
        help="Path to report root directory; validates all blocks/ files",
    )
    source.add_argument(
        "--all",
        action="store_true",
        help=(
            "Scan all report directories under reports/. "
            "Skips reports with no block files. "
            "Exits 0 with a message when no reports exist yet. "
            "--schema is auto-detected from repo_layout when omitted."
        ),
    )
    parser.add_argument(
        "--summary", action="store_true",
        help="Print block type counts per file",
    )
    args = parser.parse_args()

    # --schema required for --ndjson and --report-dir; auto-detected for --all
    if not args.all and not args.schema:
        parser.error("--schema is required when not using --all")

    schema_path = resolve_schema_path(args)
    schema      = load_schema(schema_path)
    rules       = build_rules(schema)
    top_allowed, bt_enum, top_required = get_top_level_info(schema)
    schema_ver  = get_schema_version(schema)

    # ── --all mode: scan entire reports/ tree ─────────────────────────────────
    if args.all:
        try:
            sys.path.insert(0, str(Path(__file__).resolve().parent))
            import repo_layout as rl
        except ImportError:
            print("ERROR: repo_layout.py not found — cannot use --all mode", file=sys.stderr)
            sys.exit(2)

        all_report_dirs = rl.all_report_dirs()

        if not all_report_dirs:
            print("No report directories found under reports/ — nothing to validate.")
            print("(This is expected before any reports are added.)")
            sys.exit(0)

        # Collect only reports that actually have block files
        ndjson_files: list[Path] = []
        for report_dir in all_report_dirs:
            ndjson_files.extend(rl.block_ndjson_files(report_dir))

        if not ndjson_files:
            print(f"Found {len(all_report_dirs)} report dir(s) but no content_block_*.ndjson files yet.")
            print("(Add block files when you start building report content.)")
            sys.exit(0)

    else:
        ndjson_files = resolve_ndjson_files(args)

    print(f"content_block.schema  $version={schema_ver}")
    print(f"block_types in enum   : {len(bt_enum)}")
    print(f"allOf rules parsed    : {len(rules)}")
    print(f"Files to validate     : {len(ndjson_files)}")
    print()

    total_errors  = 0
    total_blocks  = 0
    failed_files  = 0

    for ndjson_path in ndjson_files:
        errors, bt_counter = validate_ndjson(
            ndjson_path, top_allowed, top_required, bt_enum, rules
        )
        n_blocks = sum(bt_counter.values())
        total_blocks += n_blocks
        total_errors += len(errors)
        if errors:
            failed_files += 1

        print_result(ndjson_path, errors, bt_counter, args.summary)

    # Final summary
    print(f"\n{'─' * 60}")
    print(f"Total blocks  : {total_blocks}")
    print(f"Total errors  : {total_errors}")
    print(f"Files passed  : {len(ndjson_files) - failed_files}/{len(ndjson_files)}")

    if total_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
