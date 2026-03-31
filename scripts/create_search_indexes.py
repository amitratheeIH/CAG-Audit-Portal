#!/usr/bin/env python3
"""
create_search_indexes.py
------------------------
Creates MongoDB Atlas Search indexes for full-text search.
Run once before using the search feature.

Usage:
    python scripts/create_search_indexes.py --mongo-uri "mongodb+srv://..."
    python scripts/create_search_indexes.py --mongo-uri "mongodb+srv://..." --dry-run
"""

import argparse, json, os, sys, time
from pathlib import Path

DB_NAME = "cag_audit"

# Atlas Search index definitions
# These use the $search aggregation operator (Atlas Search, not $text)
SEARCH_INDEXES = [
    {
        "collection": "catalog_index",
        "name": "catalog_search",
        "definition": {
            "mappings": {
                "dynamic": False,
                "fields": {
                    "title": {
                        "type": "document",
                        "fields": {"en": [
                            {"type": "string", "analyzer": "lucene.english"},
                            {"type": "autocomplete", "analyzer": "lucene.english"},
                        ]}
                    },
                    "summary": {
                        "type": "document",
                        "fields": {"en": {"type": "string", "analyzer": "lucene.english"}}
                    },
                    "topics": {"type": "string", "analyzer": "lucene.standard"},
                    "product_id": {"type": "string", "analyzer": "lucene.keyword"},
                    "year": {"type": "number"},
                    "jurisdiction": {"type": "string", "analyzer": "lucene.keyword"},
                    "portal_section": {"type": "string", "analyzer": "lucene.keyword"},
                }
            }
        }
    },
    {
        "collection": "block_vectors",
        "name": "block_search",
        "definition": {
            "mappings": {
                "dynamic": False,
                "fields": {
                    "text_snippet": {"type": "string", "analyzer": "lucene.english"},
                    "product_id": {"type": "string", "analyzer": "lucene.keyword"},
                    "unit_id": {"type": "string", "analyzer": "lucene.keyword"},
                    "block_type": {"type": "string", "analyzer": "lucene.keyword"},
                    "para_number": {"type": "string", "analyzer": "lucene.keyword"},
                }
            }
        }
    }
]

def get_client(uri: str):
    try:
        from pymongo import MongoClient
    except ImportError:
        print("ERROR: pip install pymongo")
        sys.exit(1)
    return MongoClient(uri)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default=os.environ.get("MONGODB_URI",""),
                        help="MongoDB Atlas connection string")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not args.mongo_uri:
        print("ERROR: --mongo-uri required (or set MONGODB_URI env var)")
        sys.exit(1)

    print(f"{'DRY RUN — ' if args.dry_run else ''}Creating Atlas Search indexes\n")

    for idx in SEARCH_INDEXES:
        coll = idx["collection"]
        name = idx["name"]
        print(f"  {coll} → index '{name}'")
        if args.dry_run:
            print(f"    Would create: {json.dumps(idx['definition'], indent=2)[:120]}...")
            continue

        client = get_client(args.mongo_uri)
        db = client[DB_NAME]
        try:
            db[coll].create_search_index({
                "name": name,
                "definition": idx["definition"]
            })
            print(f"    ✓ Created (building in background — may take 1-2 min)")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"    ✓ Already exists")
            else:
                print(f"    ✗ Error: {e}")

    print("\nNote: Atlas Search indexes build asynchronously.")
    print("Check Atlas UI → Search Indexes to confirm they're ACTIVE before searching.")

if __name__ == "__main__":
    main()
