"""
repo_layout.py
--------------
Single source of truth for the CAG repository folder structure.
Import this in every script instead of hard-coding paths.

Canonical layout:

  reports/
    {product_type}/               # audit_report | accounts_report | finance_report
      {year}/                     #              | study_report | audit_impact_report | other
        {jurisdiction}/           # e.g. state, ut, union, lg
          {state_ut_code}/        # e.g. in-mp, in-jk   ← STATE, UT and LG reports ONLY
            {product_id}/         # e.g. AR06-CAG-2023-STATE-MP
            manifest.json
            metadata.json
            structure.json
            units/                # one JSON file per content_unit
            blocks/               # one NDJSON file per chapter  (content_block_*.ndjson)
            atn/                  # one JSON file per chapter    (atn_*.json)
            datasets/             # one JSON file per dataset    (DS01.json …)
              ndjson/             # NDJSON companion files for large datasets
            footnotes/            # one JSON file per chapter    (footnotes_*.json)
            pdfs/                 # PDF files, sub-structured by language
              {lang}/
                complete/
                chapters/
            assets/               # images, figures, maps
            embeddings/           # pipeline-generated sidecar NDJSON + checksum index
                                  #   (not committed to git; .gitignore'd)

  UNION jurisdiction reports have NO state_ut_code folder:
    reports/{product_type}/{year}/union/{product_id}/

  LG (Local Government) jurisdiction reports follow STATE pattern,
  using the state_ut_code of the state in which the LG operates.

State/UT folder names are the lowercase registry ID without the "IN-" prefix:
  IN-MP  →  in-mp
  IN-JK  →  in-jk
  IN-LA  →  in-la

product_type folder names match the taxonomy_product_types.json IDs exactly:
  audit_report, accounts_report, finance_report,
  study_report, audit_impact_report, compendium, other
"""

__version__ = "1.0"


from __future__ import annotations

import json
from pathlib import Path

# ── Root paths ────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = REPO_ROOT / "schemas"
TAXONOMIES_DIR = REPO_ROOT / "taxonomies"
REPORTS_DIR = REPO_ROOT / "reports"

# ── Valid product_type values — loaded dynamically from taxonomy ──────────────
# Source of truth: taxonomies/taxonomy_product_types.json
# Do NOT hardcode this list — add new product types to the taxonomy file only.

def _load_product_types() -> list[str]:
    """
    Load product_type IDs from taxonomy_product_types.json at import time.
    Falls back to an empty list if the file is missing (bootstrap / test runs).
    """
    path = TAXONOMIES_DIR / "taxonomy_product_types.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return [e["id"] for e in data.get("entries", []) if "id" in e]
    except (json.JSONDecodeError, KeyError):
        return []

PRODUCT_TYPES: list[str] = _load_product_types()

# ── Jurisdictions that get a state_ut_code subfolder ─────────────────────────

JURISDICTIONS_WITH_STATE_FOLDER = {"STATE", "UT", "LG"}

# ── Sub-folder names inside a report folder ───────────────────────────────────

REPORT_SUBFOLDERS = {
    "units":      "units",
    "blocks":     "blocks",
    "atn":        "atn",
    "datasets":   "datasets",
    "footnotes":  "footnotes",
    "pdfs":       "pdfs",
    "assets":     "assets",
    "embeddings": "embeddings",    # pipeline-generated; .gitignore'd
}

# ndjson companions live inside datasets/
NDJSON_SUBFOLDER = "datasets/ndjson"


# ── Path resolution ───────────────────────────────────────────────────────────

def state_folder_name(state_ut_id: str) -> str:
    """
    Convert a registry state_ut_id to the folder name used on disk.

    IN-MP  →  in-mp
    IN-JK  →  in-jk
    IN-LA  →  in-la
    """
    return state_ut_id.lower()  # "IN-MP" → "in-mp"


def report_dir(
    product_type: str,
    year: int,
    product_id: str,
    jurisdiction: str,
    state_ut_id: str | None = None,
) -> Path:
    """
    Return the absolute path to a report folder given its key attributes.

    For STATE / UT / LG:   reports/{product_type}/{year}/{jurisdiction}/{state_folder}/{product_id}/
    For UNION:             reports/{product_type}/{year}/union/{product_id}/
    """
    base = REPORTS_DIR / product_type / str(year)
    if jurisdiction in JURISDICTIONS_WITH_STATE_FOLDER:
        if not state_ut_id:
            raise ValueError(
                f"jurisdiction={jurisdiction} requires state_ut_id to be provided"
            )
        return base / jurisdiction.lower() / state_folder_name(state_ut_id) / product_id
    return base / jurisdiction.lower() / product_id


def report_dir_from_manifest(manifest_path: Path) -> Path:
    """Return the report folder given an absolute path to its manifest.json."""
    return manifest_path.parent


def locate_report(product_id: str) -> Path | None:
    """
    Search the entire reports/ tree for a report folder matching product_id.
    Returns the Path if found, None otherwise.

    Use this when you only know the product_id and not the full path.
    """
    for candidate in REPORTS_DIR.rglob("manifest.json"):
        if candidate.parent.name == product_id:
            return candidate.parent
    return None


def all_report_dirs() -> list[Path]:
    """
    Return all report folders in the repository, in sorted order.
    Identified by the presence of a manifest.json.
    """
    return sorted(
        m.parent for m in REPORTS_DIR.rglob("manifest.json")
    )


def product_id_from_dir(report_folder: Path) -> str:
    """The product_id is always the report folder name."""
    return report_folder.name


# ── Sub-folder helpers ────────────────────────────────────────────────────────

def units_dir(report_folder: Path) -> Path:
    return report_folder / REPORT_SUBFOLDERS["units"]

def blocks_dir(report_folder: Path) -> Path:
    return report_folder / REPORT_SUBFOLDERS["blocks"]

def atn_dir(report_folder: Path) -> Path:
    return report_folder / REPORT_SUBFOLDERS["atn"]

def datasets_dir(report_folder: Path) -> Path:
    return report_folder / REPORT_SUBFOLDERS["datasets"]

def ndjson_dir(report_folder: Path) -> Path:
    return report_folder / NDJSON_SUBFOLDER

def footnotes_dir(report_folder: Path) -> Path:
    return report_folder / REPORT_SUBFOLDERS["footnotes"]

def pdfs_dir(report_folder: Path) -> Path:
    return report_folder / REPORT_SUBFOLDERS["pdfs"]

def assets_dir(report_folder: Path) -> Path:
    return report_folder / REPORT_SUBFOLDERS["assets"]

def embeddings_dir(report_folder: Path) -> Path:
    return report_folder / REPORT_SUBFOLDERS["embeddings"]


# ── File finders ──────────────────────────────────────────────────────────────

def block_ndjson_files(report_folder: Path) -> list[Path]:
    """All content block NDJSON files: blocks/content_block_*.ndjson"""
    return sorted(blocks_dir(report_folder).glob("content_block_*.ndjson"))

def atn_json_files(report_folder: Path) -> list[Path]:
    """All ATN JSON files: atn/atn_*.json"""
    return sorted(atn_dir(report_folder).glob("atn_*.json"))

def unit_json_files(report_folder: Path) -> list[Path]:
    """All unit JSON files: units/*.json"""
    return sorted(units_dir(report_folder).glob("*.json"))

def dataset_json_files(report_folder: Path) -> list[Path]:
    """Dataset JSON files: datasets/*.json (excludes ndjson/ subfolder)"""
    return sorted(
        p for p in datasets_dir(report_folder).glob("*.json")
        if p.is_file()
    )

def footnote_json_files(report_folder: Path) -> list[Path]:
    """All footnote JSON files: footnotes/footnotes_*.json"""
    return sorted(footnotes_dir(report_folder).glob("footnotes_*.json"))

def embedding_sidecar_files(report_folder: Path) -> list[Path]:
    """All embedding sidecar NDJSON files: embeddings/embeddings_*.ndjson"""
    ed = embeddings_dir(report_folder)
    if not ed.exists():
        return []
    return sorted(ed.glob("embeddings_*.ndjson"))


# ── Manifest loading ──────────────────────────────────────────────────────────

def load_manifest(report_folder: Path) -> dict | None:
    p = report_folder / "manifest.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())

def load_metadata(report_folder: Path) -> dict | None:
    p = report_folder / "metadata.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())

def load_structure(report_folder: Path) -> dict | None:
    p = report_folder / "structure.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())
