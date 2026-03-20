#!/usr/bin/env python3
"""
write_to_atlas.py
-----------------
v1.0: Ingests validated report data into MongoDB Atlas.

Reads from:
  manifest.json, metadata.json, structure.json  (report root)
  units/      *.json
  blocks/     content_block_*.ndjson
  atn/        atn_*.json
  datasets/   *.json
  footnotes/  footnotes_*.json
  embeddings/ embeddings_*.ndjson  (pipeline-generated sidecars)

Collections written:
  report_meta     — one doc per report
  block_vectors   — one doc per content block (with embedding if available)
  atn_index       — one doc per ATN record
  catalog_index   — one doc per report, built from manifest.json + metadata.json

v1.0 changes in build_catalog_doc():
  - tabled_date (single string) → tabling_dates {lower_house: date, upper_house: date}
    Upper house date included when present (bicameral legislatures).
  - has_distributions (bool) → distributions_summary ({lang: [format, ...]})
    Language-keyed map of available formats — enables per-language download buttons
    without fetching full metadata.
  - Added: report_number {number: int, year: int}
  - Added: slug (URL-safe identifier from common_metadata)
  - Added: audit_findings_categories[] (pipeline-aggregated from section level)
  - Removed: key_findings (had no resolvable source in schema; replaced by
    audit_findings_categories which is aggregated by the pipeline)

Usage:
    python scripts/write_to_atlas.py --product-id AR06-CAG-2023-STATE-MP
    python scripts/write_to_atlas.py --all
    python scripts/write_to_atlas.py --all --dry-run
    python scripts/write_to_atlas.py --all --force
"""

__version__ = "1.1"

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import repo_layout as rl

DB_NAME = "cag_audit"
COLLECTIONS = {
    "report_meta":   "report_meta",
    "block_vectors": "block_vectors",
    "atn_index":     "atn_index",
    "catalog_index": "catalog_index",
}


def get_mongo_client():
    try:
        from pymongo import MongoClient
    except ImportError:
        print("ERROR: pymongo not installed. Run: pip install pymongo")
        sys.exit(1)
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        print("ERROR: MONGODB_URI environment variable not set")
        sys.exit(1)
    return MongoClient(uri)


def load_ndjson(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def load_embedding_sidecar(report_dir: Path) -> dict[str, list[float]]:
    embeddings: dict[str, list[float]] = {}
    for sidecar in rl.embedding_sidecar_files(report_dir):
        for row in load_ndjson(sidecar):
            if "block_id" in row and "embedding" in row:
                embeddings[row["block_id"]] = row["embedding"]
    return embeddings


def manifest_checksum(manifest: dict) -> str:
    blob = json.dumps(manifest.get("file_checksums", {}), sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:32]


def get_stored_checksum(db, product_id: str) -> str | None:
    doc = db[COLLECTIONS["report_meta"]].find_one(
        {"product_id": product_id}, {"_ingestion.manifest_checksum": 1}
    )
    return doc.get("_ingestion", {}).get("manifest_checksum") if doc else None


def build_report_meta_doc(product_id: str, report_dir: Path,
                           manifest: dict, metadata: dict,
                           structure: dict | None,
                           inheritance: dict | None = None) -> dict:
    try:
        folder_path = str(report_dir.relative_to(rl.REPO_ROOT))
    except ValueError:
        folder_path = str(report_dir)

    # Aggregate findings bottom-up from inheritance tree if available,
    # otherwise fall back to simple unit file scan
    all_findings: list[str] = []
    if inheritance:
        seen: set[str] = set()
        for resolved in inheritance.values():
            for cat in (resolved.get("audit_findings_categories") or []):
                if cat not in seen:
                    seen.add(cat)
                    all_findings.append(cat)
        all_findings = sorted(set(all_findings))
    else:
        all_findings = _aggregate_findings_from_units(report_dir)

    return {
        "product_id":   product_id,
        "product_type": manifest.get("product_type"),
        "year":         manifest.get("year"),
        "folder_path":  folder_path,
        "metadata":     metadata,
        "structure_summary": {
            "content_unit_count": len((structure or {}).get("content_units", [])),
            "front_matter_count": len((structure or {}).get("front_matter", [])),
            "back_matter_count":  len((structure or {}).get("back_matter", [])),
        },
        "audit_findings_categories": all_findings or None,
        "_ingestion": {
            "ingested_at":        datetime.now(timezone.utc).isoformat(),
            "manifest_checksum":  manifest_checksum(manifest),
            "schema_versions":    manifest.get("schema_versions", {}),
        },
    }


def _text_snippet(block: dict, max_chars: int = 500) -> str:
    """Extract a plain-text snippet for Atlas storage. Handles all block types."""
    c = block.get("content", {})
    bt = block.get("block_type", "")

    # richbox and executive_summary_block: flatten body[] items
    if bt in ("richbox", "executive_summary_block"):
        parts = []
        if title := c.get("title"):
            if isinstance(title, dict):
                parts.append(title.get("en") or next(iter(title.values()), ""))
        for item in c.get("body", []):
            t = item.get("type", "")
            if t in ("heading", "paragraph"):
                txt = item.get("text", {})
                if isinstance(txt, dict):
                    v = txt.get("en") or next(iter(txt.values()), "")
                    if v:
                        parts.append(v)
            elif t in ("bullets", "ordered_list"):
                for bullet in item.get("items", []):
                    txt = bullet.get("text", {})
                    if isinstance(txt, dict):
                        v = txt.get("en") or next(iter(txt.values()), "")
                        if v:
                            parts.append(v)
        return " ".join(parts)[:max_chars]

    # audit_finding: use observation as snippet
    if bt == "audit_finding":
        obs = c.get("observation", {})
        if isinstance(obs, dict):
            text = obs.get("en") or next(iter(obs.values()), "")
            return (text or "")[:max_chars]

    # callout / sidebar
    if bt in ("callout", "sidebar"):
        title = c.get("title", {})
        text = c.get("text", {})
        t1 = (title.get("en") or next(iter(title.values()), "")) if isinstance(title, dict) else ""
        t2 = (text.get("en") or next(iter(text.values()), "")) if isinstance(text, dict) else ""
        return (t1 + " " + t2).strip()[:max_chars]

    # Default: content.text
    text = c.get("text", {})
    if isinstance(text, dict):
        text = text.get("en") or next(iter(text.values()), "")
    return (text or "")[:max_chars]


def build_block_vector_docs(product_id: str, report_dir: Path,
                             embeddings: dict[str, list[float]],
                             inheritance: dict | None = None) -> list[dict]:
    docs = []
    for ndjson_path in rl.block_ndjson_files(report_dir):
        for block in load_ndjson(ndjson_path):
            block_id = block.get("block_id")
            if not block_id:
                continue
            doc = {
                "product_id":     product_id,
                "block_id":       block_id,
                "unit_id":        block.get("unit_id"),
                "resolved_meta":  inheritance.get(block.get("unit_id", ""), {}) if inheritance else None,
                "seq":            block.get("seq"),
                "block_type":     block.get("block_type"),
                "para_type":      block.get("content", {}).get("para_type"),
                "para_number":    block.get("para_number"),
                "audit_metadata": block.get("block_metadata"),
                "annotations":    block.get("annotations", []),
                "text_snippet":   _text_snippet(block),
            }
            if block_id in embeddings:
                doc["embedding"] = embeddings[block_id]
            docs.append(doc)
    return docs


def build_atn_docs(product_id: str, report_dir: Path) -> list[dict]:
    docs = []
    for atn_path in rl.atn_json_files(report_dir):
        data = json.loads(atn_path.read_text())
        for record in data.get("atn_records", []):
            doc = {
                "product_id":     product_id,
                "atn_id":         record.get("atn_id"),
                "chapter_id":     data.get("chapter_id"),
                "department":     record.get("department"),
                "current_status": record.get("current_status"),
                "current_round":  record.get("current_round"),
                "scope":          record.get("scope"),
                "rounds":         record.get("rounds", []),
            }
            docs.append(doc)
    return docs


def _extract_lg_code_from_product_id(product_id: str) -> str | None:
    """
    Extract the optional LG body code from a product_id.
    Pattern: {PREFIX}{NN}-CAG-{YEAR}-LG-{STATE_CODE}-{LG_CODE}
    e.g. AC01-CAG-2023-LG-MP-BBMP → BBMP
         SF01-CAG-2023-LG-MP-ZPJABALPUR → ZPJABALPUR
    Returns None if no LG code is present.
    """
    parts = product_id.split("-")
    # Find LG segment index
    try:
        lg_idx = parts.index("LG")
    except ValueError:
        return None
    # After LG: state_code (e.g. MP), then optional lg_code (e.g. BBMP)
    if len(parts) > lg_idx + 2:
        return parts[lg_idx + 2]
    return None


def _build_distributions_summary(distributions: list[dict]) -> dict[str, list[str]]:
    """
    Build distributions_summary from common_metadata.distributions[].

    Output: { "en": ["pdf", "epub"], "hi": ["pdf"] }
    Replaces the old has_distributions boolean — enables per-language download
    buttons without fetching the full metadata document.
    """
    summary: dict[str, list[str]] = {}
    for d in distributions:
        lang   = d.get("language")
        fmt    = d.get("format")
        if lang and fmt:
            summary.setdefault(lang, [])
            if fmt not in summary[lang]:
                summary[lang].append(fmt)
    return summary


# ── Inheritance rules per field (from inheritable_audit_metadata.schema) ───
# OVERRIDE  : child value completely replaces parent — last writer wins downward
# ADDITIVE  : child values are unioned with parent values (deduplicated)
# BOTTOM_UP : collected bottom-up only (section → chapter → report); never pushed down
# RESTRICTED: set only at specific levels; not propagated further
# NOT_INHERITED: scoped to the unit that set it only
_INHERIT_RULES: dict[str, str] = {
    "audit_type":                 "OVERRIDE",
    "primary_schemes":            "ADDITIVE",
    "other_schemes":              "ADDITIVE",
    "audit_period":               "OVERRIDE",
    "report_sector":              "OVERRIDE",
    "audit_findings_categories":  "BOTTOM_UP",  # section→chapter→report (additive upward)
    "regions":                    "ADDITIVE",   # sub-fields additive
    "topics":                     "ADDITIVE",
    "impact":                     "ADDITIVE",
    "main_audited_entities":      "OVERRIDE",
    "other_audited_entities":     "OVERRIDE",
    "referenced_entities":        "ADDITIVE",
    "examination_coverage":       "OVERRIDE",
    "pac_status":                 "RESTRICTED", # chapter level only
    "dpc_act_sections":           "OVERRIDE",
    "references":                 "NOT_INHERITED",
    "unit_structure":             "NOT_INHERITED",
}

def _merge_additive(parent: list | None, child: list | None) -> list | None:
    """Union two lists, preserving order (parent first), deduplicating."""
    if not parent and not child:
        return None
    seen: set = set()
    result = []
    for item in (parent or []) + (child or []):
        key = json.dumps(item, sort_keys=True) if isinstance(item, dict) else item
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result or None

def _merge_regions(parent: dict | None, child: dict | None) -> dict | None:
    """Merge regions additively per sub-field (states_uts, ulbs, pris)."""
    if not parent and not child:
        return None
    merged = {}
    for key in set(list((parent or {}).keys()) + list((child or {}).keys())):
        pv = (parent or {}).get(key)
        cv = (child or {}).get(key)
        if isinstance(pv, list) or isinstance(cv, list):
            merged[key] = _merge_additive(pv, cv)
        elif cv is not None:
            merged[key] = cv
        elif pv is not None:
            merged[key] = pv
    return merged or None

def _resolve_inherited_meta(unit_meta: dict, parent_resolved: dict) -> dict:
    """
    Compute the fully resolved metadata for a unit by applying inheritance rules.

    unit_meta       — the unit\'s own metadata (from unit JSON file)
    parent_resolved — the already-resolved metadata of the parent unit
                      (or report-level inheritable for top-level units)

    Returns a new dict with all inheritable fields resolved.
    """
    resolved = {}

    for field, rule in _INHERIT_RULES.items():
        own   = unit_meta.get(field)
        par   = parent_resolved.get(field)

        if rule == "NOT_INHERITED" or rule == "RESTRICTED":
            # Scoped to own unit only — never inherit, never propagate
            if own is not None:
                resolved[field] = own

        elif rule == "OVERRIDE":
            # Child wins if set; otherwise inherit parent
            if own is not None:
                resolved[field] = own
            elif par is not None:
                resolved[field] = par

        elif rule == "ADDITIVE":
            if field == "regions":
                merged = _merge_regions(par, own)
            else:
                merged = _merge_additive(par, own)
            if merged is not None:
                resolved[field] = merged

        elif rule == "BOTTOM_UP":
            # Only own value flows up — parent does NOT push down
            # The upward aggregation is done in build_inheritance_tree()
            if own is not None:
                resolved[field] = own

    return resolved


def build_inheritance_tree(report_dir: Path, structure: dict,
                            report_inh: dict) -> dict[str, dict]:
    """
    Walk the unit tree and compute fully resolved inherited metadata for every unit.

    Traversal is top-down (pre-order depth-first by seq within each parent group).
    Each unit\'s resolved metadata is the result of applying inheritance rules
    between its parent\'s resolved metadata and its own unit metadata.

    Returns a dict: unit_id → resolved_metadata_dict

    Also computes bottom-up aggregation (audit_findings_categories) after the
    top-down pass, rolling section-level values up through chapters to report.
    """
    # Load all unit files into a lookup
    unit_files: dict[str, dict] = {}
    units_path = report_dir / "units"
    if units_path.exists():
        for uf in units_path.glob("*.json"):
            try:
                u = json.loads(uf.read_text(encoding="utf-8"))
                uid = u.get("unit_id") or uf.stem
                unit_files[uid] = u
            except Exception:
                pass

    # Build parent→children map from structure
    all_units: list[dict] = (
        structure.get("front_matter", []) +
        structure.get("content_units", []) +
        structure.get("back_matter", [])
    )
    children_of: dict[str | None, list[dict]] = {}
    for u in all_units:
        pid = u.get("parent_id") or None
        children_of.setdefault(pid, []).append(u)

    # Sort children by seq within each parent group
    for pid in children_of:
        children_of[pid].sort(key=lambda u: u.get("seq", 0))

    resolved: dict[str, dict] = {}

    # Top-down DFS — parent_resolved starts as report-level inheritable
    def walk(unit_ids: list[dict], parent_resolved: dict):
        for u in unit_ids:
            uid = u.get("unit_id", "")
            unit_data = unit_files.get(uid, {})
            unit_meta  = unit_data.get("metadata", {})
            own_resolved = _resolve_inherited_meta(unit_meta, parent_resolved)
            resolved[uid] = own_resolved
            # Recurse into children
            walk(children_of.get(uid, []), own_resolved)

    walk(children_of.get(None, []), report_inh)

    # Bottom-up aggregation for BOTTOM_UP fields (audit_findings_categories)
    # Walk leaves → root, accumulating into parent
    bottom_up_fields = [f for f, r in _INHERIT_RULES.items() if r == "BOTTOM_UP"]
    unit_id_set = {u.get("unit_id") for u in all_units}

    def accumulate_bottom_up(uid: str):
        child_units = children_of.get(uid, [])
        for cu in child_units:
            accumulate_bottom_up(cu.get("unit_id", ""))

        for field in bottom_up_fields:
            child_values: list = []
            for cu in child_units:
                cuid = cu.get("unit_id", "")
                cv = resolved.get(cuid, {}).get(field) or []
                child_values.extend(cv)
            if child_values:
                existing = resolved[uid].get(field) or []
                merged = _merge_additive(existing, child_values)
                if merged:
                    resolved[uid][field] = merged

    for root_unit in children_of.get(None, []):
        accumulate_bottom_up(root_unit.get("unit_id", ""))

    return resolved


def _aggregate_findings_from_units(report_dir: Path) -> list[str]:
    """
    Simple aggregation of audit_findings_categories from all unit files.
    Used as fallback when structure is unavailable.
    Returns a deduplicated sorted list.
    """
    seen: set[str] = set()
    units_dir = report_dir / "units"
    if not units_dir.exists():
        return []
    for unit_file in sorted(units_dir.glob("*.json")):
        try:
            unit = json.loads(unit_file.read_text(encoding="utf-8"))
            cats = (unit.get("metadata") or {}).get("audit_findings_categories") or []
            for cat in cats:
                if isinstance(cat, str) and cat.strip():
                    seen.add(cat.strip())
        except Exception:
            continue
    return sorted(seen)


def build_catalog_doc(
    product_id: str,
    report_dir: Path,
    manifest: dict,
    metadata: dict,
    inheritance: dict | None = None,
) -> dict:
    """
    Build a catalog_index document from manifest.json + metadata.json.
    No separate catalog.json file required — derived at ingest time.

    Field sources:
      manifest        → product_id, product_type, year, has_atn, has_pdfs
      common          → title, summary, languages, default_language, canonical_url,
                        slug, supersedes, superseded_by, distributions
      report_level    → jurisdiction, audit_report_status, report_number,
                        government_context, state_ut, tabling
      inheritable     → audit_type, report_sector, topics, audit_period,
                        primary_schemes, other_schemes, regions,
                        main_audited_entities, other_audited_entities,
                        audit_findings_categories
      derived (v1.8)  → portal_section (from product_type),
                        jurisdiction_applicable,
                        accounts_metadata (for accounts_report),
                        finance_metadata (for finance_report),
                        local_body_type, lg_code (for LG jurisdiction)
    """
    common   = metadata.get("common", {})
    specific = metadata.get("specific", {})
    rl_data  = specific.get("report_level", {})
    inh      = specific.get("inheritable", {})
    tabling  = rl_data.get("tabling", {})
    file_lists = manifest.get("file_lists", {})

    # ── tabling_dates: separate lower and upper house dates (v1.0) ───────────
    # Replaces the old tabled_date single-string field (was lossy for bicameral).
    tabling_dates: dict[str, str] | None = None
    if tabling.get("applicable"):
        lh = tabling.get("lower_house", {})
        uh = tabling.get("upper_house", {})
        lh_date = lh.get("date_of_placing")
        uh_date = uh.get("date_of_placing")
        if lh_date or uh_date:
            tabling_dates = {}
            if lh_date:
                tabling_dates["lower_house"] = lh_date
            if uh_date:
                tabling_dates["upper_house"] = uh_date

    # ── distributions_summary: language-keyed format map (v1.0) ─────────────
    # Replaces the old has_distributions boolean.
    distributions = common.get("distributions", [])
    distributions_summary = _build_distributions_summary(distributions) or None

    # ── has_atn / has_pdfs ───────────────────────────────────────────────────
    has_atn  = bool(file_lists.get("atn"))
    has_pdfs = bool(file_lists.get("pdfs")) or bool(rl_data.get("pdf_assets"))

    # ── report_path ──────────────────────────────────────────────────────────
    try:
        report_path = str(report_dir.relative_to(rl.REPO_ROOT))
    except ValueError:
        report_path = str(report_dir)

    # ── Required base fields ─────────────────────────────────────────────────
    doc: dict = {
        "product_id":       product_id,
        "product_type":     manifest.get("product_type"),
        "title":            common.get("title"),
        "year":             manifest.get("year"),
        "default_language": common.get("default_language"),
        "languages":        common.get("languages", []),
        "jurisdiction":     rl_data.get("jurisdiction"),
        "audit_status":     rl_data.get("audit_report_status"),
        "has_atn":          has_atn,
        "has_pdfs":         has_pdfs,
        "report_path":      report_path,
        "last_indexed":     datetime.now(timezone.utc).isoformat(),
    }

    # ── Optional fields — only include when values are present ───────────────
    _opt: dict = {
        # Identity / navigation
        "report_number":      rl_data.get("report_number"),          # {number, year}
        "slug":               common.get("slug"),                     # URL-safe identifier
        "summary":            common.get("summary"),
        "canonical_url":      common.get("canonical_url"),
        "supersedes":         common.get("supersedes"),
        "superseded_by":      common.get("superseded_by"),
        # Tabling (v1.0: replaces single tabled_date)
        "tabling_dates":      tabling_dates,
        # Distributions (v1.0: replaces has_distributions boolean)
        "distributions_summary": distributions_summary,
        # Audit classification
        "audit_type":         inh.get("audit_type") or None,
        "report_sector":      inh.get("report_sector") or None,
        "topics":             inh.get("topics") or None,
        "audit_period":       inh.get("audit_period"),
        # Findings: use inheritance tree (bottom-up aggregated) if available,
        # then metadata.json inheritable, then simple unit file scan
        "audit_findings_categories": (
            (sorted(set(
                cat
                for resolved in (inheritance or {}).values()
                for cat in (resolved.get("audit_findings_categories") or [])
            )) or None)
            or inh.get("audit_findings_categories")
            or _aggregate_findings_from_units(report_dir)
            or None
        ),
        # Schemes and geography
        "primary_schemes":    inh.get("primary_schemes") or None,
        "other_schemes":      inh.get("other_schemes") or None,
        "regions":            inh.get("regions"),
        # Entities
        "main_audited_entities":  inh.get("main_audited_entities") or None,
        "other_audited_entities": inh.get("other_audited_entities") or None,
        "government_context":     rl_data.get("government_context"),
    }
    for k, v in _opt.items():
        if v is not None:
            doc[k] = v

    # ── portal_section (v1.8) ─────────────────────────────────────────────────
    # Derived from product_type + jurisdiction + local_body_type.
    # Six values: audit_reports, accounts_reports, finance_reports,
    # study_reports, audit_impact, compendium.
    product_type  = manifest.get("product_type", "")
    jurisdiction  = rl_data.get("jurisdiction", "")

    _PORTAL_MAP = {
        "audit_report":        "audit_reports",
        "study_report":        "study_reports",
        "audit_impact_report": "audit_impact",
        "compendium":          "compendium",
        "other":               "compendium",
        "accounts_report":     "accounts_reports",
        "finance_report":      "finance_reports",
    }
    doc["portal_section"] = _PORTAL_MAP.get(product_type, "compendium")
    doc["jurisdiction_applicable"] = product_type in (
        "audit_report", "accounts_report", "finance_report"
    )

    # ── accounts_metadata (v1.8) ─────────────────────────────────────────────
    # Present only for accounts_report. Populated from specific.accounts_metadata
    # if present, otherwise derived from jurisdiction + local_body_type.
    if product_type == "accounts_report":
        acc_spec = specific.get("accounts_metadata", {})
        local_body_type = acc_spec.get("local_body_type") or specific.get("local_body_type")
        lg_code = common.get("lg_code") or _extract_lg_code_from_product_id(product_id)

        # Derive accounts_category from jurisdiction + local_body_type
        if jurisdiction == "UNION":
            acc_cat = "union_accounts"
        elif jurisdiction in ("STATE", "UT"):
            acc_cat = "state_accounts"
        elif jurisdiction == "LG":
            acc_cat = "ulb_accounts" if local_body_type == "ulb" else "pri_accounts"
        else:
            acc_cat = None

        accounts_meta: dict = {}
        if acc_cat:
            accounts_meta["accounts_category"] = acc_cat
        for field in ("accounts_sub_type", "finance_year", "certification_opinion",
                      "audited_entity", "certification_date"):
            val = acc_spec.get(field)
            if val:
                accounts_meta[field] = val
        if local_body_type:
            accounts_meta["local_body_type"] = local_body_type
            doc["local_body_type"] = local_body_type
        if lg_code:
            doc["lg_code"] = lg_code
        if accounts_meta:
            doc["accounts_metadata"] = accounts_meta

    # ── finance_metadata (v1.8) ──────────────────────────────────────────────
    # Present only for finance_report.
    elif product_type == "finance_report":
        fin_spec = specific.get("finance_metadata", {})
        local_body_type = fin_spec.get("local_body_type") or specific.get("local_body_type")
        lg_code = common.get("lg_code") or _extract_lg_code_from_product_id(product_id)

        if jurisdiction == "UNION":
            fin_cat = "union_finance"
        elif jurisdiction in ("STATE", "UT"):
            fin_cat = "state_finance"
        elif jurisdiction == "LG":
            fin_cat = "ulb_finance" if local_body_type == "ulb" else "pri_finance"
        else:
            fin_cat = None

        finance_meta: dict = {}
        if fin_cat:
            finance_meta["finance_category"] = fin_cat
        for field in ("finance_year", "fiscal_deficit_pct", "revenue_deficit",
                      "primary_deficit", "debt_ratio"):
            val = fin_spec.get(field)
            if val is not None:
                finance_meta[field] = val
        if local_body_type:
            finance_meta["local_body_type"] = local_body_type
            doc["local_body_type"] = local_body_type
        if lg_code:
            doc["lg_code"] = lg_code
        if finance_meta:
            doc["finance_metadata"] = finance_meta

    return doc


def upsert_collection(db, collection_name: str, docs: list[dict],
                      id_field: str, dry_run: bool) -> int:
    if not docs or dry_run:
        return len(docs) if dry_run else 0
    from pymongo import UpdateOne
    ops = [
        UpdateOne({id_field: doc[id_field]}, {"$set": doc}, upsert=True)
        for doc in docs
        if id_field in doc
    ]
    if ops:
        result = db[collection_name].bulk_write(ops)
        return result.upserted_count + result.modified_count
    return 0


def ingest_report(report_dir: Path, db, force: bool, dry_run: bool) -> dict:
    product_id = rl.product_id_from_dir(report_dir)
    stats = {"status": "ok", "blocks": 0, "atn": 0, "catalog": 0}

    manifest = rl.load_manifest(report_dir)
    if not manifest:
        stats["status"] = "skip (no manifest)"
        return stats

    if not force and not dry_run:
        stored = get_stored_checksum(db, product_id)
        if stored == manifest_checksum(manifest):
            stats["status"] = "skip (unchanged)"
            return stats

    metadata   = rl.load_metadata(report_dir) or {}
    structure  = rl.load_structure(report_dir)
    embeddings = load_embedding_sidecar(report_dir)

    # Build inheritance tree — resolves all inheritable fields for every unit
    report_inh = (metadata.get("specific") or {}).get("inheritable") or {}
    inheritance = build_inheritance_tree(report_dir, structure or {}, report_inh)

    meta_doc = build_report_meta_doc(product_id, report_dir, manifest, metadata,
                                     structure, inheritance)
    if not dry_run:
        db[COLLECTIONS["report_meta"]].update_one(
            {"product_id": product_id}, {"$set": meta_doc}, upsert=True
        )

    block_docs = build_block_vector_docs(product_id, report_dir, embeddings, inheritance)
    stats["blocks"] = upsert_collection(
        db, COLLECTIONS["block_vectors"], block_docs, "block_id", dry_run
    )

    atn_docs = build_atn_docs(product_id, report_dir)
    stats["atn"] = upsert_collection(
        db, COLLECTIONS["atn_index"], atn_docs, "atn_id", dry_run
    )

    catalog_doc = build_catalog_doc(product_id, report_dir, manifest, metadata, inheritance)
    stats["catalog"] = upsert_collection(
        db, COLLECTIONS["catalog_index"], [catalog_doc], "product_id", dry_run
    )

    return stats


def resolve_dirs(args) -> list[Path]:
    if args.all:
        return rl.all_report_dirs()
    ids = []
    if args.product_ids:
        ids = [pid.strip() for pid in args.product_ids.split(",")]
    elif args.product_id:
        ids = [args.product_id.strip()]
    dirs = []
    for pid in ids:
        found = rl.locate_report(pid)
        if not found:
            print(f"WARN: product_id '{pid}' not found — skipping")
        else:
            dirs.append(found)
    return dirs


def main():
    parser = argparse.ArgumentParser(description="Write reports to MongoDB Atlas")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--product-id",  help="Single product_id")
    group.add_argument("--product-ids", help="Comma-separated product_ids")
    group.add_argument("--all", action="store_true")
    parser.add_argument("--force",   action="store_true", help="Re-ingest even if checksum unchanged")
    parser.add_argument("--dry-run", action="store_true", help="Log without writing")
    args = parser.parse_args()

    db   = None if args.dry_run else get_mongo_client()[DB_NAME]
    dirs = resolve_dirs(args)

    if not dirs:
        print("No report directories to process.")
        sys.exit(0)

    print(f"{'DRY RUN — ' if args.dry_run else ''}Ingesting {len(dirs)} report(s)\n")

    for report_dir in dirs:
        stats = ingest_report(report_dir, db, args.force, args.dry_run)
        try:
            label = str(report_dir.relative_to(rl.REPO_ROOT))
        except ValueError:
            label = str(report_dir)

        status = stats["status"]
        if status.startswith("skip"):
            print(f"SKIP  {label}  [{status}]")
        else:
            print(
                f"OK    {label}  "
                f"blocks={stats['blocks']}  atn={stats['atn']}  catalog={stats['catalog']}"
                f"{'  [DRY RUN]' if args.dry_run else ''}"
            )

    print(f"\n{'─'*60}\nDone.")


if __name__ == "__main__":
    main()
