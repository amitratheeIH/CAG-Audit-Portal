#!/usr/bin/env python3
"""
validate_registry_refs.py
-------------------------
v1.0: Checks that every registry ID referenced in report files exists in the
corresponding registry / taxonomy file.
v1.1: Added validate_block_ndjson_files() — validates annotations[].target
for cross_reference type (content_block.schema v1.1+).

Checks:
  - state_ut.id                        → registry_states_uts.json
  - examination_coverage.state_ut_ids[]→ registry_states_uts.json
  - regions.states_uts[]               → registry_states_uts.json
  - main_audited_entities[]            → registry_entities.json
  - other_audited_entities[]           → registry_entities.json
  - primary_schemes[]                  → registry_schemes.json
  - other_schemes[]                    → registry_schemes.json
  - report_sector[]                    → taxonomy_report_sector.json
  - audit_type[]                       → taxonomy_audit_type.json
  - topics[]                           → taxonomy_topics.json
  - product_type                       → taxonomy_product_types.json
  - audit_findings_categories[]        → taxonomy_audit_findings_{product_type}.json
  - government_context.nodal_ministry  → registry_entities.json
  - government_context.nodal_departments[] → registry_entities.json

v1.0 changes:
  - entity.function_ids[] validated against taxonomy_entity_functions.json
    (function_ids are on entity registry entries, not on report metadata — validated
    as part of check_registry_integrity.py, not here)
  - state_ut.id validated against registry_states_uts.json (unchanged)
  - audit_findings_categories[] now validated at unit/section level (unchanged logic)

Usage:
    python scripts/validate_registry_refs.py
    python scripts/validate_registry_refs.py --product-id AR06-CAG-2023-STATE-MP
"""

__version__ = "1.1"

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import repo_layout as rl


def load_registry_ids(filename: str, id_field: str = "id") -> set[str]:
    path = rl.TAXONOMIES_DIR / filename
    data = json.loads(path.read_text())
    entries = data.get("entries", data) if isinstance(data, dict) else data
    return {e[id_field] for e in entries if id_field in e}


FINDINGS_TAXONOMY_FILENAME = "taxonomy_audit_findings_{product_type}.json"


def audit_findings_taxonomy_filename(product_type: str) -> str:
    return FINDINGS_TAXONOMY_FILENAME.format(product_type=product_type)


def load_audit_findings_ids(product_type: str | None) -> set[str] | None:
    if not product_type:
        return None
    fname = audit_findings_taxonomy_filename(product_type)
    path = rl.TAXONOMIES_DIR / fname
    if not path.exists():
        return None
    return load_registry_ids(fname)


def load_registries() -> dict[str, set[str]]:
    return {
        "states_uts":    load_registry_ids("registry_states_uts.json"),
        "entities":      load_registry_ids("registry_entities.json"),
        "schemes":       load_registry_ids("registry_schemes.json"),
        "report_sector": load_registry_ids("taxonomy_report_sector.json"),
        "audit_type":    load_registry_ids("taxonomy_audit_type.json"),
        "product_type":  load_registry_ids("taxonomy_product_types.json"),
        "topics":        load_registry_ids("taxonomy_topics.json"),
    }


def check_ref(value: str, registry: set[str], label: str, source: str, errors: list[str]):
    if value and value not in registry:
        errors.append(f"  [{source}] Unknown {label}: '{value}'")


def check_refs(values: list, registry: set[str], label: str, source: str, errors: list[str]):
    for v in (values or []):
        check_ref(v, registry, label, source, errors)


def _entity_fields(entry: dict | str) -> dict[str, list[str] | str | None]:
    """Normalise an audited_entities entry — handles both object and legacy string forms."""
    if isinstance(entry, dict):
        return entry
    return {"ministry": None, "department": entry, "autonomous_bodies": [], "other_bodies": []}


def validate_inheritable(obj: dict, registries: dict, source: str,
                         audit_findings_ids: set[str] | None = None) -> list[str]:
    errors: list[str] = []
    r = registries

    check_refs(obj.get("report_sector", []), r["report_sector"], "report_sector", source, errors)
    check_refs(obj.get("audit_type", []),    r["audit_type"],    "audit_type",    source, errors)
    check_refs(obj.get("topics", []),        r["topics"],        "topics",        source, errors)

    for entry in obj.get("main_audited_entities", []):
        e = _entity_fields(entry)
        check_ref(e.get("ministry"),    r["entities"], "main_audited_entities.ministry",    source, errors)
        check_ref(e.get("department"),  r["entities"], "main_audited_entities.department",  source, errors)
        check_refs(e.get("autonomous_bodies", []), r["entities"], "main_audited_entities.autonomous_bodies", source, errors)
        check_refs(e.get("other_bodies", []),       r["entities"], "main_audited_entities.other_bodies",       source, errors)

    for entry in obj.get("other_audited_entities", []):
        e = _entity_fields(entry)
        check_ref(e.get("ministry"),   r["entities"], "other_audited_entities.ministry",   source, errors)
        check_ref(e.get("department"), r["entities"], "other_audited_entities.department", source, errors)
        check_refs(e.get("autonomous_bodies", []), r["entities"], "other_audited_entities.autonomous_bodies", source, errors)
        check_refs(e.get("other_bodies", []),       r["entities"], "other_audited_entities.other_bodies",       source, errors)

    check_refs(obj.get("referenced_entities", []), r["entities"], "referenced_entities", source, errors)
    check_refs(obj.get("primary_schemes", []),     r["schemes"],  "primary_schemes",     source, errors)
    check_refs(obj.get("other_schemes", []),        r["schemes"],  "other_schemes",       source, errors)

    check_refs(
        obj.get("regions", {}).get("states_uts", []),
        r["states_uts"],
        "regions.states_uts",
        source,
        errors,
    )
    coverage = obj.get("examination_coverage", {})
    if isinstance(coverage, dict):
        check_refs(
            coverage.get("state_ut_ids", []),
            r["states_uts"],
            "examination_coverage.state_ut_ids",
            source,
            errors,
        )

    if audit_findings_ids is not None:
        check_refs(
            obj.get("audit_findings_categories", []),
            audit_findings_ids,
            "audit_findings_categories",
            source,
            errors,
        )

    return errors


def validate_metadata_file(report_dir: Path, registries: dict,
                            audit_findings_ids: set[str] | None) -> list[str]:
    errors: list[str] = []
    metadata = rl.load_metadata(report_dir)
    if not metadata:
        return errors

    common   = metadata.get("common", {})
    specific = metadata.get("specific", {})
    rl_data  = specific.get("report_level", {})

    # state_ut.id
    state_ut = rl_data.get("state_ut", {})
    if isinstance(state_ut, dict):
        check_ref(state_ut.get("id"), registries["states_uts"], "state_ut.id", "metadata.json", errors)

    # government_context
    gov_ctx = rl_data.get("government_context", {})
    if isinstance(gov_ctx, dict):
        check_ref(gov_ctx.get("nodal_ministry"), registries["entities"],
                  "government_context.nodal_ministry", "metadata.json", errors)
        check_refs(gov_ctx.get("nodal_departments", []), registries["entities"],
                   "government_context.nodal_departments", "metadata.json", errors)

    # tabling: submitted_to[] when not tabled
    tabling = rl_data.get("tabling", {})
    if isinstance(tabling, dict):
        check_refs(tabling.get("submitted_to", []), registries["entities"],
                   "tabling.submitted_to", "metadata.json", errors)

    # report-level inheritable
    inheritable = specific.get("inheritable", {})
    if inheritable:
        errors.extend(validate_inheritable(
            inheritable, registries, "metadata.json/specific/inheritable",
            audit_findings_ids=None   # not at section level
        ))

    return errors


def validate_structure_node(node: dict, registries: dict, source: str, errors: list[str],
                             audit_findings_ids: set[str] | None = None):
    if "metadata" in node:
        node_id = node.get("unit_id", "?")
        errors.extend(validate_inheritable(
            node["metadata"], registries, f"{source}/{node_id}",
            audit_findings_ids=audit_findings_ids
        ))


def validate_structure_file(report_dir: Path, registries: dict,
                             audit_findings_ids: set[str] | None) -> list[str]:
    errors: list[str] = []
    structure = rl.load_structure(report_dir)
    if not structure:
        return errors

    for section_key in ("front_matter", "content_units", "back_matter"):
        for unit in structure.get(section_key, []):
            validate_structure_node(unit, registries, "structure.json", errors, audit_findings_ids)
    return errors


def validate_unit_files(report_dir: Path, registries: dict,
                         audit_findings_ids: set[str] | None) -> list[str]:
    errors: list[str] = []
    for unit_file in rl.unit_json_files(report_dir):
        try:
            unit = json.loads(unit_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        metadata = unit.get("metadata", {})
        if metadata:
            errors.extend(validate_inheritable(
                metadata, registries, f"units/{unit_file.name}",
                audit_findings_ids=audit_findings_ids
            ))
    return errors


def validate_block_ndjson_files(report_dir: Path) -> list[str]:
    """
    Validate annotations[].target for cross_reference blocks.
    Loads structure.json unit_ids for intra-report resolution.
    Added v1.1 for content_block.schema v1.1+ cross_reference annotation support.
    """
    errors: list[str] = []

    # Load all unit_ids from structure for intra-report target resolution
    known_unit_ids: set[str] = set()
    structure = rl.load_structure(report_dir)
    if structure:
        def collect_ids(units_list: list) -> None:
            for u in (units_list or []):
                uid = u.get("unit_id")
                if uid:
                    known_unit_ids.add(uid)
                collect_ids(u.get("children_units", []))
        for section in ("front_matter", "content_units", "back_matter"):
            collect_ids(structure.get(section, []))

    import re as _re
    _cross_ref_target_re = _re.compile(
        r"^([A-Z0-9][A-Z0-9\-]+[A-Z0-9])$"  # bare unit_id / block_id
        r"|^([A-Z0-9][A-Z0-9\-]+)/(.+)$"     # product_id/something
        r"|^https?://"                          # URI
    )

    for ndjson_path in rl.block_ndjson_files(report_dir):
        fname = f"blocks/{ndjson_path.name}"
        for lineno, line in enumerate(ndjson_path.read_text().splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                block = json.loads(line)
            except json.JSONDecodeError:
                continue

            block_id = block.get("block_id", f"line {lineno}")
            for ann in block.get("annotations", []):
                if ann.get("annotation_type") != "cross_reference":
                    continue
                target = ann.get("target")
                # target must be present
                if not target:
                    errors.append(
                        f"  [{fname}] block {block_id} annotation_type=cross_reference "
                        f"missing required 'target' field"
                    )
                    continue
                # target format must match one of the valid patterns
                if not _cross_ref_target_re.match(target):
                    errors.append(
                        f"  [{fname}] block {block_id} cross_reference target "
                        f"'{target}' does not match expected format "
                        f"(unit_id, product_id/unit_id, or URI)"
                    )
                    continue
                # intra-report bare unit_id: resolve against structure
                bare_match = _re.match(r"^([A-Z0-9][A-Z0-9\-]+[A-Z0-9])$", target)
                if bare_match and known_unit_ids and target not in known_unit_ids:
                    # Could be a block_id — only warn, not error
                    errors.append(
                        f"  [{fname}] block {block_id} cross_reference target "
                        f"'{target}' not found in structure.json unit_ids "
                        f"(may be a block_id — verify manually)"
                    )

    return errors


def validate_report_dir(report_dir: Path, registries: dict) -> dict[str, list[str]]:
    all_errors: dict[str, list[str]] = {}

    manifest = rl.load_manifest(report_dir)
    product_type = (manifest or {}).get("product_type")

    # Validate product_type itself
    if product_type and product_type not in registries["product_type"]:
        all_errors.setdefault("manifest.json", []).append(
            f"  [manifest.json] Unknown product_type: '{product_type}'"
        )

    audit_findings_ids = load_audit_findings_ids(product_type)
    if product_type and audit_findings_ids is None:
        fname = audit_findings_taxonomy_filename(product_type)
        all_errors.setdefault("_warnings", []).append(
            f"  [warn] {fname} not found — audit_findings_categories not validated"
        )

    for validate_fn, key in [
        (lambda: validate_metadata_file(report_dir, registries, audit_findings_ids), "metadata.json"),
        (lambda: validate_structure_file(report_dir, registries, audit_findings_ids), "structure.json"),
        (lambda: validate_unit_files(report_dir, registries, audit_findings_ids), "units/"),
        (lambda: validate_block_ndjson_files(report_dir), "blocks/"),
    ]:
        errs = validate_fn()
        if errs:
            all_errors[key] = errs

    return all_errors


def main():
    parser = argparse.ArgumentParser(description="Validate registry references in report files")
    parser.add_argument("--product-id", help="Single product_id to check (searches full tree)")
    args = parser.parse_args()

    registries = load_registries()
    failed = 0

    if args.product_id:
        found = rl.locate_report(args.product_id)
        if not found:
            print(f"ERROR: product_id '{args.product_id}' not found under reports/")
            sys.exit(1)
        dirs = [found]
    else:
        dirs = rl.all_report_dirs()

    if not dirs:
        print("No report directories found.")
        sys.exit(0)

    for report_dir in dirs:
        errors = validate_report_dir(report_dir, registries)
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
