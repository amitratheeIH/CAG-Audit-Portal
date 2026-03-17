#!/usr/bin/env python3
"""
check_registry_integrity.py
---------------------------
v1.0: Validates internal consistency of all registry and taxonomy files.

  registry_states_uts.json
    - No duplicate IDs
    - successor/predecessor IDs exist within the registry
    - Inactive entries (active=false) have status_history entries
      NOTE: states/UTs do not use dissolved_on — status changes are tracked
      in status_history[]. Only check dissolved_on for entity and scheme files.

  registry_entities.json
    - No duplicate IDs
    - parent_id references exist within the registry
    - predecessor/successor IDs exist within the registry
    - Dissolved/archived entries have dissolved_on + dissolution_reason
    - level/entity_type pairing is consistent per level_entity_type_mapping

  registry_schemes.json
    - No duplicate IDs
    - predecessor_id / successor_id / related_id references exist within registry
    - sector_function_id uses FUNC-* prefix (v1.0: replaces free-text sector field)
    - administering_entity_id matches entity_id pattern

  taxonomy_entity_functions.json  (new in v1.0)
    - No duplicate IDs
    - All IDs use FUNC-* prefix

  taxonomy_report_sector.json
    - No duplicate IDs
    - sub-sector parent_id references exist within the taxonomy
    - sub_sectors[] arrays reference valid IDs
    - cag_wing values are from the controlled cag_wing_values list

  taxonomy_audit_type.json
    - No duplicate IDs

  taxonomy_product_types.json
    - No duplicate IDs

  taxonomy_topics.json
    - No duplicate IDs
    - parent_id references exist
    - sub_topics[] references exist
    - short_label.en does not end with truncation characters (&, ,, ' -')
    - level consistency (topic → parent_id=null, sub_topic → parent_id set)

  taxonomy_audit_findings_*.json
    - No duplicate IDs
    - parent_id references exist
    - sub_categories[] references exist
    - level consistency (category → parent_id=null)

Usage:
    python scripts/check_registry_integrity.py
"""

__version__ = "1.0"

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TAXONOMIES_DIR = REPO_ROOT / "taxonomies"

ENTITY_ID_PATTERN = re.compile(r'^[A-Z0-9][A-Z0-9\-]+[A-Z0-9]$')
FUNC_ID_PATTERN   = re.compile(r'^FUNC-[A-Z][A-Z0-9\-]+$')
STATE_ID_PATTERN  = re.compile(r'^IN-[A-Z]{2,3}$')

OK = True


def fail(msg: str):
    global OK
    OK = False
    print(f"  FAIL  {msg}")


def warn(msg: str):
    print(f"  WARN  {msg}")


def info(label: str, count: int, extra: str = ""):
    print(f"  OK    {label} — {count} entries{' ' + extra if extra else ''}")


def get_entries(data) -> list[dict]:
    if isinstance(data, list):
        return data
    return data.get("entries", [])


def check_no_duplicates(entries: list[dict], id_field: str, label: str) -> set[str]:
    seen: set[str] = set()
    dupes: list[str] = []
    for e in entries:
        eid = e.get(id_field)
        if eid in seen:
            dupes.append(eid)
        seen.add(eid)
    if dupes:
        fail(f"{label}: duplicate IDs: {dupes}")
    return seen


def check_refs(entries: list[dict], ref_fields: list[str], known_ids: set[str], label: str):
    for e in entries:
        eid = e.get("id", "?")
        for field in ref_fields:
            val = e.get(field)
            if val is None:
                continue
            vals = val if isinstance(val, list) else [val]
            for v in vals:
                if v and v not in known_ids:
                    fail(f"{label} [{eid}].{field} references unknown ID: '{v}'")


def check_truncated_labels(entries: list[dict], label: str):
    """Catch short_label values that were truncated mid-string."""
    TRUNCATION_MARKERS = ('&', ',', ' -', ' —')
    for e in entries:
        sl = (e.get("short_label") or {}).get("en", "")
        if sl and any(sl.endswith(m) for m in TRUNCATION_MARKERS):
            fail(f"{label} [{e.get('id','?')}]: short_label.en appears truncated: '{sl}'")


# ── States/UTs ────────────────────────────────────────────────────────────────

def check_states_uts():
    path = TAXONOMIES_DIR / "registry_states_uts.json"
    print(f"\n{path.name}")
    data = json.loads(path.read_text())
    entries = get_entries(data)
    ids = check_no_duplicates(entries, "id", path.name)

    # All IDs must match IN-XX pattern
    for e in entries:
        if not STATE_ID_PATTERN.match(e.get("id", "")):
            fail(f"{path.name} [{e.get('id','?')}]: ID does not match ^IN-[A-Z]{{2,3}}$")

    check_refs(entries, ["predecessor_ids", "successor_ids"], ids, path.name)

    # Inactive states: must have at least one status_history entry
    # NOTE: states do NOT use dissolved_on — status changes are in status_history[]
    for e in entries:
        if e.get("active") is False:
            if not e.get("status_history"):
                fail(f"{path.name} [{e['id']}]: inactive entry has no status_history entries")

    # Each entry must have status_history with at least one item
    for e in entries:
        if not e.get("status_history"):
            fail(f"{path.name} [{e['id']}]: missing status_history (required for pipeline computed status_at_report_date)")

    # legislature field must not be present (removed in v1.0)
    for e in entries:
        if "legislature" in e:
            fail(f"{path.name} [{e['id']}]: 'legislature' field must not be present (removed in v1.0 — derive from current_status)")

    info(path.name, len(entries))


# ── Entities ──────────────────────────────────────────────────────────────────

def check_entities():
    path = TAXONOMIES_DIR / "registry_entities.json"
    print(f"\n{path.name}")
    data = json.loads(path.read_text())
    entries = get_entries(data)
    ids = check_no_duplicates(entries, "id", path.name)

    # ID pattern check
    for e in entries:
        if not ENTITY_ID_PATTERN.match(e.get("id", "")):
            fail(f"{path.name} [{e.get('id','?')}]: ID does not match entity_id pattern")

    check_refs(entries, ["parent_id"], ids, path.name)
    check_refs(entries, ["predecessor_ids", "successor_ids"], ids, path.name)

    # level/entity_type consistency
    mapping = data.get("level_entity_type_mapping", {})
    if mapping:
        for e in entries:
            level = e.get("level", "")
            etype = e.get("entity_type", "")
            valid_types = mapping.get(level)
            if valid_types and etype not in valid_types:
                fail(f"{path.name} [{e['id']}]: level='{level}' but entity_type='{etype}' "
                     f"(expected one of {valid_types})")

    # Dissolved/archived entities must have dissolution fields
    for e in entries:
        if not e.get("active", True) or e.get("archived"):
            if not e.get("dissolved_on"):
                fail(f"{path.name} [{e['id']}]: archived/inactive entry missing dissolved_on")
            if not e.get("dissolution_reason"):
                fail(f"{path.name} [{e['id']}]: archived/inactive entry missing dissolution_reason")

    # function_ids must use FUNC-* pattern
    for e in entries:
        for fid in e.get("function_ids", []):
            if not FUNC_ID_PATTERN.match(fid):
                fail(f"{path.name} [{e['id']}]: function_id '{fid}' does not match ^FUNC-[A-Z]...$")

    info(path.name, len(entries))


# ── Schemes ───────────────────────────────────────────────────────────────────

def check_schemes():
    path = TAXONOMIES_DIR / "registry_schemes.json"
    print(f"\n{path.name}")
    data = json.loads(path.read_text())
    entries = get_entries(data)
    ids = check_no_duplicates(entries, "id", path.name)

    check_refs(entries, ["predecessor_ids", "successor_ids", "related_ids"], ids, path.name)

    # sector_function_id must use FUNC-* prefix (v1.0: replaces free-text sector)
    for e in entries:
        sfid = e.get("sector_function_id")
        if sfid is None:
            fail(f"{path.name} [{e.get('id','?')}]: missing sector_function_id (v1.0: replaces free-text 'sector')")
        elif not FUNC_ID_PATTERN.match(sfid):
            fail(f"{path.name} [{e.get('id','?')}]: sector_function_id '{sfid}' must match ^FUNC-[A-Z]...$")

        # Old sector field must not be present
        if "sector" in e:
            fail(f"{path.name} [{e.get('id','?')}]: deprecated 'sector' field present (renamed to sector_function_id in v1.0)")

        # administering_entity_id must match entity_id pattern
        aeid = e.get("administering_entity_id")
        if aeid and not ENTITY_ID_PATTERN.match(aeid):
            fail(f"{path.name} [{e.get('id','?')}]: administering_entity_id '{aeid}' does not match entity_id pattern")

    info(path.name, len(entries))


# ── Entity Functions Taxonomy (new in v1.0) ───────────────────────────────────

def check_entity_functions():
    path = TAXONOMIES_DIR / "taxonomy_entity_functions.json"
    if not path.exists():
        print(f"\ntaxonomy_entity_functions.json  (MISSING — required in v1.0)")
        fail("taxonomy_entity_functions.json not found in taxonomies/")
        return
    print(f"\n{path.name}")
    data = json.loads(path.read_text())
    entries = get_entries(data)
    ids = check_no_duplicates(entries, "id", path.name)

    # All IDs must use FUNC-* prefix
    for e in entries:
        if not FUNC_ID_PATTERN.match(e.get("id", "")):
            fail(f"{path.name} [{e.get('id','?')}]: ID must match ^FUNC-[A-Z][A-Z0-9\\-]+$")

    # Must have label.en on every entry
    for e in entries:
        if not (e.get("label") or {}).get("en"):
            fail(f"{path.name} [{e.get('id','?')}]: missing label.en")

    info(path.name, len(entries))


# ── Report Sector Taxonomy ────────────────────────────────────────────────────

def check_report_sector():
    path = TAXONOMIES_DIR / "taxonomy_report_sector.json"
    print(f"\n{path.name}")
    data = json.loads(path.read_text())
    entries = get_entries(data)
    ids = check_no_duplicates(entries, "id", path.name)

    check_refs(entries, ["parent_id"], ids, path.name)

    # sub_sectors[] arrays must reference valid IDs
    for e in entries:
        for sid in e.get("sub_sectors", []):
            if sid not in ids:
                fail(f"{path.name} [{e['id']}].sub_sectors references unknown ID: '{sid}'")

    # cag_wing must be from the controlled list
    valid_wings = set(data.get("cag_wing_values", []))
    if valid_wings:
        for e in entries:
            wing = e.get("cag_wing")
            if wing and wing not in valid_wings:
                fail(f"{path.name} [{e['id']}]: cag_wing '{wing}' not in cag_wing_values")

    # Sector-level entries must have sub_sectors (warn, not fail, for new sectors)
    for e in entries:
        if e.get("level") == "sector" and not e.get("sub_sectors"):
            warn(f"{path.name} [{e['id']}]: sector entry has no sub_sectors defined")

    n_sectors = sum(1 for e in entries if e.get("level") == "sector")
    n_sub     = sum(1 for e in entries if e.get("level") == "sub_sector")
    info(path.name, len(entries), f"({n_sectors} sectors, {n_sub} sub-sectors)")


# ── Audit Type Taxonomy ───────────────────────────────────────────────────────

def check_audit_type():
    path = TAXONOMIES_DIR / "taxonomy_audit_type.json"
    print(f"\n{path.name}")
    data = json.loads(path.read_text())
    entries = get_entries(data)
    check_no_duplicates(entries, "id", path.name)

    # All IDs must use ATYPE-* prefix
    for e in entries:
        if not e.get("id", "").startswith("ATYPE-"):
            fail(f"{path.name} [{e.get('id','?')}]: ID must start with ATYPE-")

    info(path.name, len(entries))


# ── Product Types Taxonomy ────────────────────────────────────────────────────

def check_product_types():
    path = TAXONOMIES_DIR / "taxonomy_product_types.json"
    print(f"\n{path.name}")
    data = json.loads(path.read_text())
    entries = get_entries(data)
    check_no_duplicates(entries, "id", path.name)
    info(path.name, len(entries))


# ── Topics Taxonomy ───────────────────────────────────────────────────────────

def check_topics():
    path = TAXONOMIES_DIR / "taxonomy_topics.json"
    if not path.exists():
        print(f"\n{path.name}  (SKIPPED — file not present)")
        return
    print(f"\n{path.name}")
    data = json.loads(path.read_text())
    entries = get_entries(data)
    ids = check_no_duplicates(entries, "id", path.name)

    check_refs(entries, ["parent_id"], ids, path.name)
    check_truncated_labels(entries, path.name)

    for e in entries:
        eid = e.get("id", "?")
        for sid in e.get("sub_topics", []):
            if sid not in ids:
                fail(f"{path.name} [{eid}].sub_topics references unknown ID: '{sid}'")

    for e in entries:
        level = e.get("level")
        if level == "topic" and e.get("parent_id") is not None:
            fail(f"{path.name} [{e['id']}]: level=topic but has parent_id set")
        if level == "sub_topic" and not e.get("parent_id"):
            fail(f"{path.name} [{e['id']}]: level=sub_topic but parent_id is null/missing")

    n_topics = sum(1 for e in entries if e.get("level") == "topic")
    n_sub    = sum(1 for e in entries if e.get("level") == "sub_topic")
    info(path.name, len(entries), f"({n_topics} topics, {n_sub} sub-topics)")


# ── Audit Findings Taxonomies ─────────────────────────────────────────────────

def check_audit_findings():
    pattern = "taxonomy_audit_findings_*.json"
    findings_files = sorted(TAXONOMIES_DIR.glob(pattern))
    if not findings_files:
        print(f"\ntaxonomy_audit_findings_*.json  (SKIPPED — no files present)")
        return

    for path in findings_files:
        print(f"\n{path.name}")
        data = json.loads(path.read_text())
        entries = get_entries(data)
        ids = check_no_duplicates(entries, "id", path.name)

        check_refs(entries, ["parent_id"], ids, path.name)

        for e in entries:
            eid = e.get("id", "?")
            for sid in e.get("sub_categories", []):
                if sid not in ids:
                    fail(f"{path.name} [{eid}].sub_categories references unknown ID: '{sid}'")

        for e in entries:
            level = e.get("level")
            pid = e.get("parent_id")
            if level == "category" and pid is not None:
                fail(f"{path.name} [{e['id']}]: level=category but has parent_id set")
            if level in ("sub_category", "detail") and not pid:
                fail(f"{path.name} [{e['id']}]: level={level} but parent_id is null/missing")

        if "product_types" not in data:
            fail(f"{path.name}: missing product_types[] field at root")

        n_cat = sum(1 for e in entries if e.get("level") == "category")
        n_sub = sum(1 for e in entries if e.get("level") == "sub_category")
        n_det = sum(1 for e in entries if e.get("level") == "detail")
        pt    = data.get("product_types", [])
        print(f"  OK    {path.name} — {len(entries)} entries "
              f"({n_cat} categories, {n_sub} sub-categories, {n_det} detail)  "
              f"product_types={pt}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Registry & Taxonomy Integrity Check  (v{__version__})")
    print("=" * 50)

    check_states_uts()
    check_entities()
    check_schemes()
    check_entity_functions()       # new in v1.0
    check_report_sector()
    check_audit_type()
    check_product_types()
    check_topics()
    check_audit_findings()

    print(f"\n{'─'*50}")
    if OK:
        print("All checks passed.")
    else:
        print("One or more integrity checks FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
