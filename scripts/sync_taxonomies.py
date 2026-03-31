#!/usr/bin/env python3
"""
sync_taxonomies.py
------------------
Writes taxonomy entries into MongoDB Atlas.
Run this once from the portal repo whenever taxonomy files change.

Usage:
    python scripts/sync_taxonomies.py --taxonomies-dir "C:/path/to/taxonomy-repo/taxonomies"
    python scripts/sync_taxonomies.py --taxonomies-dir "C:/path/to/taxonomy-repo/taxonomies" --dry-run

Collections written to MongoDB (db: cag_audit):
    taxonomy_afc    ← taxonomy_audit_findings_audit_report.json
    taxonomy_topics ← taxonomy_topics.json
"""

import argparse
import json
import os
import sys
from pathlib import Path

DB_NAME = "cag_audit"

TAXONOMY_FILES = {
    "taxonomy_afc":           "taxonomy_audit_findings_audit_report.json",
    "taxonomy_topics":        "taxonomy_topics.json",
    "taxonomy_report_sector": "taxonomy_report_sector.json",
    "taxonomy_audit_type":    "taxonomy_audit_type.json",
}


def get_db(uri: str):
    try:
        from pymongo import MongoClient
    except ImportError:
        print("ERROR: pymongo not installed.  Run:  pip install pymongo")
        sys.exit(1)
    return MongoClient(uri)[DB_NAME]


def sync_collection(db, collection_name: str, json_path: Path, dry_run: bool) -> int:
    if not json_path.exists():
        print(f"  SKIP  {json_path}  (file not found)")
        return 0

    # Always read as UTF-8 — taxonomy files contain Hindi/Devanagari characters
    data    = json.loads(json_path.read_text(encoding="utf-8"))
    entries = data.get("entries", [])
    if not entries:
        print(f"  WARN  {json_path.name}  (no entries)")
        return 0

    if dry_run:
        print(f"  DRY RUN  {json_path.name}  →  {collection_name}  ({len(entries)} entries)")
        return len(entries)

    from pymongo import UpdateOne
    ops = [UpdateOne({"id": e["id"]}, {"$set": e}, upsert=True) for e in entries]
    result = db[collection_name].bulk_write(ops, ordered=False)
    n = result.upserted_count + result.modified_count
    print(f"  OK  {json_path.name}  →  {collection_name}  ({n} upserted/modified)")
    return n


def main():
    parser = argparse.ArgumentParser(description="Sync taxonomy JSON files to MongoDB Atlas")
    parser.add_argument(
        "--taxonomies-dir",
        required=True,
        help='Path to the folder containing the taxonomy JSON files.\n'
             'Example: "C:/Users/ict/Desktop/report Metadata/taxonomy-repo/taxonomies"',
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.environ.get("MONGODB_URI", ""),
        help="MongoDB connection string (mongodb+srv://...). "
             "Can also be set via MONGODB_URI environment variable.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print what would be written without touching MongoDB")
    args = parser.parse_args()

    tax_dir = Path(args.taxonomies_dir)
    if not tax_dir.exists():
        print(f"ERROR: --taxonomies-dir does not exist: {tax_dir}")
        sys.exit(1)

    if not args.dry_run and not args.mongo_uri:
        print("ERROR: MongoDB URI required. Pass it with --mongo-uri or set MONGODB_URI env var.")
        print('  Example: python scripts/sync_taxonomies.py --taxonomies-dir "..." --mongo-uri "mongodb+srv://..."')
        sys.exit(1)

    db = get_db(args.mongo_uri) if not args.dry_run else None
    print(f"{'DRY RUN — ' if args.dry_run else ''}Syncing taxonomies to MongoDB\n")

    for collection_name, filename in TAXONOMY_FILES.items():
        sync_collection(db, collection_name, tax_dir / filename, args.dry_run)

    if not args.dry_run:
        for coll in TAXONOMY_FILES:
            db[coll].create_index("id", unique=True, background=True)
        print("\nIndexes ensured.")

    print("\nDone.")


if __name__ == "__main__":
    main()
